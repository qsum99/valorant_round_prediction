"""
server.py  —  Valorant Win Predictor Backend
=============================================
Reads Overwolf JSON log events in real-time, builds round state,
runs Model A (pre-round) and Model B (live) predictions, and pushes
probability updates to the React overlay via WebSocket.

Architecture:
  Overwolf → writes JSON log → server reads it → models predict → WebSocket → React overlay

Usage:
    python backend/server.py

Then open the React overlay — it connects to ws://localhost:8765
"""

import asyncio
import json
import os
import pickle
import time
import logging
import websockets
from pathlib import Path

import numpy as np
import pandas as pd

from buy_advisor import BuyAdvisor

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("valo-backend")

# ── Config ────────────────────────────────────────────────────────────────────
WS_HOST        = "127.0.0.1"
WS_PORT        = 8765
MODELS_DIR     = Path(__file__).parent.parent / "models"
LOG_WATCH_DIR  = Path(__file__).parent.parent / "raw_matchs_data"
POLL_INTERVAL  = 0.5   # seconds between log file checks

# ── Feature definitions (overridden by metadata.json if present) ─────────────
A_FEATURES = [
    "att_money", "def_money", "economy_diff",
    "att_full_buy", "def_full_buy", "att_eco", "def_eco",
    "att_ults_ready", "def_ults_ready", "ult_adv",
    "score_won", "score_lost", "score_diff",
    "round_number", "local_team_side", "map",
]
B_FEATURES = [
    "att_money", "def_money", "economy_diff",
    "att_full_buy", "def_full_buy", "att_eco", "def_eco",
    "att_ults_ready", "def_ults_ready", "ult_adv",
    "score_won", "score_lost", "score_diff",
    "att_alive", "def_alive", "alive_diff", "alive_ratio",
    "att_kills", "def_kills", "kill_diff",
    "spike_planted", "kill_index", "kill_progress",
    "round_number", "local_team_side", "map",
]

# ── Encodings (must match training exactly) ───────────────────────────────────
MAP_ENCODING = {
    "Ascent": 0, "Bonsai": 1, "Foxtrot": 2, "Jam": 3,
    "Juliett": 4, "Plummet": 5, "Triad": 6,
}
SIDE_ENCODING = {"attack": 1, "defense": 0}
MAX_TEAM_MONEY = 45_000
PLAYER_COUNT   = 5
FULL_BUY_THRESHOLD = 19500
ECO_THRESHOLD      = 10000

# ── Overwolf agent codenames → display names ──────────────────────────────────
AGENT_NAMES = {
    "Clay": "Raze", "Pandemic": "Viper", "Wraith": "Omen", "Hunter": "Sova",
    "Thorne": "Sage", "Phoenix": "Phoenix", "Wushu": "Jett", "Gumshoe": "Cypher",
    "Sarge": "Brimstone", "Breach": "Breach", "Vampire": "Reyna",
    "Killjoy": "Killjoy", "Guide": "Skye", "Stealth": "Yoru", "Rift": "Astra",
    "Grenadier": "KAY/O", "Deadeye": "Chamber", "Sprinter": "Neon",
    "BountyHunter": "Fade", "Mage": "Harbor", "AggroBot": "Gekko",
    "Cable": "Deadlock", "Sequoia": "Iso", "Smonk": "Clove", "Nox": "Vyse",
    "Cashew": "Tejo", "Terra": "Waylay",
}


def resolve_agent_name(raw):
    """Map an Overwolf agent codename to its display name."""
    if not raw:
        return raw
    s = str(raw)
    for code in sorted(AGENT_NAMES, key=len, reverse=True):
        if code.lower() in s.lower():
            return AGENT_NAMES[code]
    import re
    return re.sub(r"(_PC_C|_PostDeath)$", "", s)


# ── Overwolf rank codes → full tier names ──────────────────────────────────────
RANK_NAMES = {
    0: "Unranked", 3: "Iron 1", 4: "Iron 2", 5: "Iron 3",
    6: "Bronze 1", 7: "Bronze 2", 8: "Bronze 3",
    9: "Silver 1", 10: "Silver 2", 11: "Silver 3",
    12: "Gold 1", 13: "Gold 2", 14: "Gold 3",
    15: "Platinum 1", 16: "Platinum 2", 17: "Platinum 3",
    18: "Diamond 1", 19: "Diamond 2", 20: "Diamond 3",
    21: "Ascendant 1", 22: "Ascendant 2", 23: "Ascendant 3",
    24: "Immortal 1", 25: "Immortal 2", 26: "Immortal 3",
    27: "Radiant",
}


def format_rank(rank) -> str:
    """Convert an Overwolf roster rank (code 0-27 or tier name string) to a full tier name."""
    if rank is None:
        return "Unranked"
    if isinstance(rank, str):
        s = rank.strip()
        if not s or s.lower() in ("none", "null", "unknown", "unranked", "?"):
            return "Unranked"
        for name in RANK_NAMES.values():
            if s.lower() == name.lower():
                return name
        try:
            return RANK_NAMES.get(int(s), s)
        except ValueError:
            return s
    try:
        return RANK_NAMES.get(int(rank), "Unranked")
    except (ValueError, TypeError):
        return "Unranked"


def parse_round_report(raw):
    """Parse the Overwolf per-round report into a numeric summary dict or None."""
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, dict):
        return None

    def num(v):
        try:
            return float(v) if v is not None else 0.0
        except (ValueError, TypeError):
            return 0.0

    return {
        "damage": num(raw.get("damage")),
        "hit": num(raw.get("hit")),
        "headshot": num(raw.get("headshot")),
        "bodyshots": num(raw.get("bodyshots")),
        "legshots": num(raw.get("legshots")),
        "final_headshot": num(raw.get("final_headshot")),
        "damage_received": num(raw.get("damage_received")),
        "hits_received": num(raw.get("hits_received")),
        "ability_damage": num(raw.get("ability_damage")),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Model loader
# ══════════════════════════════════════════════════════════════════════════════

def load_models():
    model_a_path = MODELS_DIR / "model_a.pkl"
    model_b_path = MODELS_DIR / "model_b.pkl"
    meta_path    = MODELS_DIR / "metadata.json"

    if not model_a_path.exists() or not model_b_path.exists():
        raise FileNotFoundError(
            f"Models not found in {MODELS_DIR}. "
            "Run train.ipynb first to generate model_a.pkl and model_b.pkl"
        )

    with open(model_a_path, "rb") as f:
        model_a = pickle.load(f)
    with open(model_b_path, "rb") as f:
        model_b = pickle.load(f)

    # Load feature lists from metadata if available
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            global A_FEATURES, B_FEATURES
            if "a_features" in meta:
                A_FEATURES = meta["a_features"]
            if "b_features" in meta:
                B_FEATURES = meta["b_features"]
            log.info(f"📋 Features loaded from metadata.json (A={len(A_FEATURES)}, B={len(B_FEATURES)})")
        except Exception as e:
            log.warning(f"Could not load metadata.json: {e}")

    log.info(f"✅ Models loaded from {MODELS_DIR}")
    return model_a, model_b


# ══════════════════════════════════════════════════════════════════════════════
# Round state tracker
# ══════════════════════════════════════════════════════════════════════════════

class RoundState:
    """
    Accumulates Overwolf streaming events into a complete round state.
    Mirrors the same logic as extract_features.py but runs in real-time.
    """

    def __init__(self):
        self.reset()

    def reset(self, keep_side=False):
        self.raw: dict = {}
        self.team_comp_snapshot: dict = {}
        self.round_number:    int   = 0
        if not keep_side:
            self.map_name:          str = ""
            self.local_side:        str = ""
            self.local_player_name: str = ""
            self.local_agent:       str = ""
        self.phase:           str   = ""
        self.score_won:       int   = 0
        self.score_lost:      int   = 0
        self.prev_score_won:  int   = 0
        self.prev_score_lost: int   = 0
        self.pre_snap:        dict  = {}
        self.att_alive:       int   = PLAYER_COUNT
        self.def_alive:       int   = PLAYER_COUNT
        self.att_kills:       int   = 0
        self.def_kills:       int   = 0
        self.kill_index:      int   = 0
        self.spike_planted:   bool  = False
        self.planted_this_round: bool = False
        self.last_round_planted: bool = False
        self.spike_site:      str   = ""
        self.spike_carrier:   str   = ""
        self.ally_streak:     int   = 0
        self.enemy_streak:    int   = 0
        self.last_round_report: dict | str | None = None
        self.pre_round_prob:  float = 0.5
        self.live_prob:       float = 0.5
        self.last_buy_rec:    dict | None = None

    def update_from_info(self, data: dict):
        mi = data.get("match_info", data) if isinstance(data, dict) else {}
        if not isinstance(mi, dict):
            mi = {}

        for k, v in mi.items():
            if v is not None:
                self.raw[k] = v

        # Track per-round report explicitly (value may be null to signal "no data")
        if "round_report" in mi:
            self.last_round_report = mi.get("round_report")

        # Check "me" object if present in data or mi
        me_data = data.get("me") if isinstance(data, dict) else None
        if not me_data and isinstance(mi, dict):
            me_data = mi.get("me")
        if me_data:
            if isinstance(me_data, str):
                try:
                    me_data = json.loads(me_data)
                except Exception:
                    pass
            if isinstance(me_data, dict):
                if me_data.get("player_name"):
                    self.local_player_name = str(me_data["player_name"]).split("#")[0].strip()

        # Check scoreboard and roster for local player and character
        for k, v in mi.items():
            if (k.startswith("scoreboard_") or k.startswith("roster_")) and v:
                try:
                    p = json.loads(v) if isinstance(v, str) else v
                    if isinstance(p, dict):
                        if p.get("local") is True or p.get("is_local") is True:
                            if p.get("name"):
                                self.local_player_name = str(p["name"]).split("#")[0].strip()
                            if p.get("character") or p.get("agent"):
                                self.local_agent = resolve_agent_name(p.get("character") or p.get("agent"))
                except Exception:
                    pass

        if "round_phase" in mi:
            self.phase = mi["round_phase"]

        if self.raw.get("round_number"):
            try:
                self.round_number = int(self.raw["round_number"])
            except (ValueError, TypeError):
                pass

        if self.raw.get("map"):
            raw_map = str(self.raw["map"]).strip()
            if raw_map and raw_map.lower() != "null":
                MAP_NAME_MAPPINGS = {
                    "infinity": "Abyss", "triad": "Haven", "duality": "Bind",
                    "bonsai": "Split", "ascent": "Ascent", "port": "Icebox",
                    "foxtrot": "Breeze", "canyon": "Fracture", "pitt": "Pearl",
                    "jam": "Lotus", "juliett": "Sunset", "rook": "Corrode",
                    "range": "Practice Range", "hurr_alley": "District",
                    "hurr_yard": "Piazza", "hurr_bowl": "Kasbah",
                    "hurr_helix": "Drift", "hurr_hightide": "Glitch",
                }
                self.map_name = MAP_NAME_MAPPINGS.get(raw_map.lower(), raw_map)

        if self.raw.get("team"):
            val = str(self.raw["team"]).lower()
            if "def" in val or "blue" in val:
                self.local_side = "defense"
            elif "att" in val or "red" in val:
                self.local_side = "attack"
            else:
                self.local_side = val

        if self.raw.get("score"):
            try:
                sc = json.loads(self.raw["score"]) if isinstance(self.raw["score"], str) else self.raw["score"]
                self.score_won  = sc.get("won",  0)
                self.score_lost = sc.get("lost", 0)
            except Exception:
                pass

    def get_scoreboard(self):
        boards = []
        for i in range(10):
            raw = self.raw.get(f"scoreboard_{i}")
            if raw:
                try:
                    p = json.loads(raw) if isinstance(raw, str) else raw
                    boards.append(p)
                except Exception:
                    pass

        allies  = [p for p in boards if p.get("teammate") is True]
        enemies = [p for p in boards if p.get("teammate") is False]

        # Fill default fallback player data if scoreboard is partially populated in early rounds
        default_money = 800 if self.round_number <= 1 else 3000
        while len(allies) < PLAYER_COUNT:
            allies.append({"money": default_money, "ult_max": 7, "ult_points": 0})
        while len(enemies) < PLAYER_COUNT:
            enemies.append({"money": default_money, "ult_max": 7, "ult_points": 0})

        side = self.local_side if self.local_side in ("attack", "defense") else "attack"
        if side == "attack":
            return allies, enemies
        else:
            return enemies, allies

    def capture_pre_round_snapshot(self):
        att, dff = self.get_scoreboard()
        att_money = min(sum((p.get("money") or 0) for p in att), MAX_TEAM_MONEY)
        def_money = min(sum((p.get("money") or 0) for p in dff), MAX_TEAM_MONEY)
        att_ults  = sum(1 for p in att if (p.get("ult_max") or 0) > 0 and (p.get("ult_points") or 0) >= (p.get("ult_max") or 1))
        def_ults  = sum(1 for p in dff if (p.get("ult_max") or 0) > 0 and (p.get("ult_points") or 0) >= (p.get("ult_max") or 1))

        side_val = 1 if self.local_side == "attack" else (0 if self.local_side == "defense" else 1)

        self.pre_snap = {
            "att_money"      : att_money,
            "def_money"      : def_money,
            "economy_diff"   : att_money - def_money,
            "att_full_buy"   : int(att_money >= FULL_BUY_THRESHOLD),
            "def_full_buy"   : int(def_money >= FULL_BUY_THRESHOLD),
            "att_eco"        : int(att_money < ECO_THRESHOLD),
            "def_eco"        : int(def_money < ECO_THRESHOLD),
            "att_ults_ready" : att_ults,
            "def_ults_ready" : def_ults,
            "ult_adv"        : att_ults - def_ults,
            "score_won"      : self.score_won,
            "score_lost"     : self.score_lost,
            "score_diff"     : self.score_won - self.score_lost,
            "round_number"   : max(1, self.round_number),
            "local_team_side": side_val,
            "map"            : MAP_ENCODING.get(self.map_name, 0),
        }

        self.att_alive     = PLAYER_COUNT
        self.def_alive     = PLAYER_COUNT
        self.att_kills     = 0
        self.def_kills     = 0
        self.kill_index    = 0
        self.spike_planted = False
        self.spike_site    = ""
        self.spike_carrier = ""
        return True

    def on_kill(self, kf: dict) -> dict:
        if not self.pre_snap:
            self.capture_pre_round_snapshot()

        self.kill_index += 1

        is_att_kill = (
            (self.local_side == "attack"  and kf.get("is_attacker_teammate")) or
            (self.local_side == "defense" and not kf.get("is_attacker_teammate"))
        )

        if is_att_kill:
            self.att_kills += 1
            self.def_alive  = max(0, self.def_alive - 1)
        else:
            self.def_kills += 1
            self.att_alive  = max(0, self.att_alive - 1)

        total_alive = self.att_alive + self.def_alive
        alive_ratio = self.att_alive / (total_alive if total_alive > 0 else 1)
        alive_diff  = self.att_alive - self.def_alive

        return {
            **self.pre_snap,
            "pre_round_prob": self.pre_round_prob,
            "att_alive"    : self.att_alive,
            "def_alive"    : self.def_alive,
            "alive_diff"   : alive_diff,
            "alive_ratio"  : round(alive_ratio, 4),
            "att_kills"    : self.att_kills,
            "def_kills"    : self.def_kills,
            "kill_diff"    : self.att_kills - self.def_kills,
            "spike_planted": int(self.spike_planted),
            "kill_index"   : self.kill_index,
            "kill_progress": round(self.kill_index / 10.0, 4),
        }

    def on_spike_planted(self, site: str = "", carrier: str = ""):
        self.spike_planted = True
        self.planted_this_round = True
        self.spike_site = site or ""
        self.spike_carrier = carrier or ""

    def on_spike_cleared(self):
        self.spike_planted = False
        self.spike_site = ""
        self.spike_carrier = ""

    def build_team_comp(self) -> dict:
        """Build {allies, enemies} team lists from roster/scoreboard data (name, agent, rank).

        Only named (non-observer, non-wiped) entries are used; allies are identified
        via the teammate flag or team code 1. Gaps (missing players or wiped ranks)
        are filled from the match-start snapshot, which holds the full ranked roster.
        """
        allies, enemies, seen = [], [], set()
        for i in range(20):
            raw = self.raw.get(f"roster_{i}") or self.raw.get(f"scoreboard_{i}")
            if not raw:
                continue
            try:
                p = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(p, dict) or not str(p.get("name") or "").strip():
                continue
            pid = p.get("player_id") or p.get("id") or p.get("puuid")
            key = pid or str(p["name"]).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            name  = str(p.get("name") or "?").split("#")[0].strip() or "?"
            agent = resolve_agent_name(p.get("character") or p.get("agent") or "")
            if agent in (None, "", "None", "null", "?"):
                agent = "?"
            rank  = format_rank(p.get("rank"))
            entry = {"name": name, "agent": agent, "rank": rank}
            if p.get("teammate") is True:
                allies.append(entry)
            else:
                enemies.append(entry)

        snap = self.team_comp_snapshot or {}
        if snap:
            snap_all = snap.get("allies", []) + snap.get("enemies", [])
            snap_by_name = {p["name"]: p for p in snap_all}
            names = {p["name"] for p in allies + enemies}
            for p in snap.get("allies", []):
                if len(allies) < 5 and p["name"] not in names:
                    allies.append(p); names.add(p["name"])
            for p in snap.get("enemies", []):
                if len(enemies) < 5 and p["name"] not in names:
                    enemies.append(p); names.add(p["name"])
            for p in allies + enemies:
                cached = snap_by_name.get(p["name"])
                if not cached:
                    continue
                if p["rank"] == "Unranked" and cached["rank"] != "Unranked":
                    p["rank"] = cached["rank"]
                if p["agent"] == "?" and cached["agent"] != "?":
                    p["agent"] = cached["agent"]

        return {"allies": allies, "enemies": enemies}


# ══════════════════════════════════════════════════════════════════════════════
# Match Recorder & History Accumulator (for post-match report)
# ══════════════════════════════════════════════════════════════════════════════

FORCE_BUY_THRESHOLD = 14000  # between eco and full-buy


class MatchRecorder:
    """Accumulates per-round data, live kill timelines, and model metrics for post-match reports."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.rounds = []          # list of completed round dicts
        self._current_round = {}  # in-progress round data

    def on_shopping_phase(self, round_num: int, side: str, score_won: int, score_lost: int,
                          pre_prob: float, ally_money: int, enemy_money: int, buy_rec: dict | str = None):
        """Called at shopping/buy phase."""
        if round_num <= 1 and score_won == 0 and score_lost == 0:
            buy_type = "pistol"
        elif round_num == 13:
            buy_type = "pistol"
        elif ally_money < ECO_THRESHOLD:
            buy_type = "eco"
        elif ally_money < FORCE_BUY_THRESHOLD:
            buy_type = "force"
        else:
            buy_type = "full_buy"

        rec_str = buy_type
        if isinstance(buy_rec, dict):
            rec_str = buy_rec.get("recommendation", buy_type)
        elif isinstance(buy_rec, str):
            rec_str = buy_rec

        self._current_round = {
            "round_number": round_num,
            "round": round_num,
            "side": side,
            "score_before": [score_won, score_lost],
            "pre_prob": round(pre_prob * 100, 1),
            "ally_money": ally_money,
            "enemy_money": enemy_money,
            "buy_type": buy_type,
            "buy_recommendation": rec_str,
            "kills": [],
            "kills_by_local": 0,
            "deaths_by_local": 0,
            "player_kills": 0,
            "player_deaths": 0,
        }

    # Alias for backward compatibility
    record_round_start = on_shopping_phase

    def on_kill(self, kf_data: dict, live_prob: float, local_player_name: str,
                is_ally_kill: bool, att_alive: int, def_alive: int):
        """Called on each kill_feed event."""
        if not self._current_round:
            return

        attacker = kf_data.get("attacker", "?")
        victim   = kf_data.get("victim", "?")
        headshot = bool(kf_data.get("headshot", False))

        # Check if local player was killer or victim
        att_clean = str(attacker).split("#")[0].strip().lower()
        vic_clean = str(victim).split("#")[0].strip().lower()
        loc_clean = str(local_player_name).split("#")[0].strip().lower() if local_player_name else ""

        if loc_clean:
            if loc_clean in att_clean or att_clean in loc_clean:
                self._current_round["kills_by_local"] += 1
                self._current_round["player_kills"] += 1
            if loc_clean in vic_clean or vic_clean in loc_clean:
                self._current_round["deaths_by_local"] += 1
                self._current_round["player_deaths"] += 1

        kill_entry = {
            "kill_index": len(self._current_round.get("kills", [])) + 1,
            "attacker": attacker,
            "victim": victim,
            "headshot": headshot,
            "att_alive": att_alive,
            "def_alive": def_alive,
            "live_prob": round(live_prob * 100, 1),
            "is_attacker_teammate": bool(is_ally_kill),
        }
        self._current_round.setdefault("kills", []).append(kill_entry)

    def record_kill(self, is_ally_kill: bool):
        """Legacy helper."""
        pass

    def on_round_end(self, round_num: int, score_won: int, score_lost: int,
                     won: bool, final_prob: float, round_report: dict | None = None):
        """Called at end phase."""
        if not self._current_round:
            return

        cr = self._current_round
        if round_report:
            cr["round_report"] = round_report
        score_before_won = cr.get("score_before", [0, 0])[0]
        if score_won > score_before_won:
            is_won = True
        elif won:
            is_won = True
        else:
            is_won = False

        cr["score_after"] = [score_won, score_lost]
        cr["result"] = "win" if is_won else "loss"
        cr["won"] = is_won
        cr["final_prob"] = round(final_prob * 100, 1)

        pre = cr.get("pre_prob", 50.0)
        if is_won and pre < 40:
            cr["performance"] = "clutch"
        elif not is_won and pre > 60:
            cr["performance"] = "choke"
        else:
            cr["performance"] = "expected"

        if is_won:
            cr["prob_swing"] = round(100 - pre, 1)
        else:
            cr["prob_swing"] = round(-pre, 1)

        self.rounds.append(cr)
        self._current_round = {}

    # Alias for backward compatibility
    record_round_end = on_round_end

    def get_report_data(self, map_name: str, outcome: str, final_score_won: int, final_score_lost: int,
                        local_agent: str = "", local_player_name: str = "") -> dict:
        """Returns the full data dictionary required by the HTML report generator."""
        import uuid
        from datetime import datetime

        # Calculate model accuracy (fraction of rounds correctly predicted by Model A)
        correct_count = 0
        for r in self.rounds:
            pre = r.get("pre_prob", 50.0)
            is_won = r.get("won", False)
            if (pre >= 50.0 and is_won) or (pre < 50.0 and not is_won):
                correct_count += 1
        model_accuracy = (correct_count / len(self.rounds)) if self.rounds else 0.0

        match_id = str(uuid.uuid4())[:8]
        date_str = datetime.now().strftime("%Y-%m-%d")

        return {
            "match": {
                "match_id": match_id,
                "map": map_name or "Valorant",
                "outcome": (outcome or "victory").lower(),
                "final_score": [final_score_won, final_score_lost],
                "date": date_str,
                "local_agent": local_agent or "Agent",
                "local_player_name": local_player_name or "Player",
                "model_accuracy": round(model_accuracy, 3),
            },
            "rounds": self.rounds,
        }

    def generate_report(self, map_name, outcome, final_score_won, final_score_lost,
                        local_agent: str = "", local_player_name: str = "",
                        team_comp: dict | None = None):
        """Generates JSON post-match report payload for overlay compatibility."""
        from datetime import datetime

        if not self.rounds:
            return None

        # --- Model A accuracy: fraction of rounds correctly predicted ---
        correct_count = 0
        for r in self.rounds:
            pre = r.get("pre_prob", 50.0)
            is_won = r.get("won", False)
            if (pre >= 50.0 and is_won) or (pre < 50.0 and not is_won):
                correct_count += 1
        model_accuracy = (correct_count / len(self.rounds)) if self.rounds else 0.0

        # --- Max win streak (consecutive won rounds) ---
        max_streak = 0
        current_streak = 0
        for r in self.rounds:
            if r.get("won"):
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        # --- Biggest upset: won round with the lowest pre-round odds ---
        won_rounds = [r for r in self.rounds if r.get("won")]
        biggest_upset = None
        if won_rounds:
            u = min(won_rounds, key=lambda r: r.get("pre_prob", 100))
            biggest_upset = {
                "round": u.get("round_number", u.get("round")),
                "pre_prob": u.get("pre_prob"),
                "swing": u.get("prob_swing"),
            }

        # --- Buy advisor evaluation: MATCH when play followed the recommendation ---
        for r in self.rounds:
            rec = str(r.get("buy_recommendation") or "").strip().lower()
            actual = str(r.get("buy_type") or "").strip().lower()
            r["buy_eval"] = "MATCH" if (rec and rec == actual) else "DIFF"

        # --- Pivotal rounds: largest absolute prob_swing ---
        sorted_by_swing = sorted(
            self.rounds,
            key=lambda r: abs(r.get("prob_swing", 0)),
            reverse=True
        )
        pivotal = []
        for r in sorted_by_swing[:3]:
            swing = r.get("prob_swing", 0)
            if r["won"]:
                if r["pre_prob"] < 40:
                    reason = f"Clutch! Won with only {r['pre_prob']}% odds"
                elif r["buy_type"] == "eco":
                    reason = "Huge eco round win — great economy swing"
                else:
                    reason = "Key round win that shifted momentum"
            else:
                if r["pre_prob"] > 60:
                    reason = f"Choked with {r['pre_prob']}% odds — should have won"
                elif r["buy_type"] == "full_buy":
                    reason = "Lost full-buy — economy wasted"
                else:
                    reason = "Key round loss that shifted momentum"
            pivotal.append({
                "round": r["round_number"],
                "swing": round(abs(swing), 1),
                "reason": reason,
                "won": r["won"],
                "pre_prob": r["pre_prob"],
            })

        # --- Economy efficiency by buy category (BuyAdvisor states) ---
        advisor_cats = ("pistol", "eco", "force", "half_buy", "full_buy", "bonus", "anti_eco", "broken")
        economy = {}
        for bt in advisor_cats:
            bt_rounds = [r for r in self.rounds
                         if str(r.get("buy_recommendation") or r.get("buy_type") or "").strip().lower() == bt]
            economy[bt] = {
                "played": len(bt_rounds),
                "won": sum(1 for r in bt_rounds if r.get("won")),
            }

        return {
            "type": "match_report",
            "map": map_name,
            "outcome": outcome,
            "final_score": [final_score_won, final_score_lost],
            "date": datetime.now().strftime("%Y-%m-%d"),
            "local_agent": local_agent or "Agent",
            "local_player_name": local_player_name or "Player",
            "team_comp": team_comp or {},
            "model_accuracy": round(model_accuracy, 3),
            "max_streak": max_streak,
            "biggest_upset": biggest_upset,
            "rounds": self.rounds,
            "pivotal_rounds": pivotal,
            "economy": economy,
        }


MatchHistory = MatchRecorder


# ══════════════════════════════════════════════════════════════════════════════
# Predictor
# ══════════════════════════════════════════════════════════════════════════════

class Predictor:
    def __init__(self, model_a, model_b):
        self.model_a = model_a
        self.model_b = model_b

    def predict_pre_round(self, snap: dict) -> float:
        try:
            x = pd.DataFrame([snap])[A_FEATURES].astype(float)
            att_prob = float(self.model_a.predict_proba(x)[0][1])
            local_side = int(snap.get("local_team_side", 1))
            ally_prob = att_prob if local_side == 1 else (1.0 - att_prob)
            return max(0.01, min(0.99, float(ally_prob)))
        except Exception as e:
            log.warning(f"Model A prediction failed: {e}")
            return 0.5

    def predict_live(self, live_row: dict) -> float:
        try:
            att_alive  = int(live_row.get("att_alive", 5))
            def_alive  = int(live_row.get("def_alive", 5))
            local_side = int(live_row.get("local_team_side", 1))

            ally_alive  = att_alive if local_side == 1 else def_alive
            enemy_alive = def_alive if local_side == 1 else att_alive

            # Absolute game constraints
            if ally_alive == 0:
                return 0.0
            if enemy_alive == 0:
                return 1.0

            # Base prediction from Model B (predicts Attacker Win Probability)
            x = pd.DataFrame([live_row])[B_FEATURES].astype(float)
            att_mb_prob = float(self.model_b.predict_proba(x)[0][1])

            # Convert Model B prediction to Ally Win Probability
            ally_mb_prob = att_mb_prob if local_side == 1 else (1.0 - att_mb_prob)

            # Dynamic live manpower shift based on active player difference (Ally vs Enemy)
            alive_diff = ally_alive - enemy_alive
            shift = alive_diff * 0.18
            if live_row.get("spike_planted", 0) == 1:
                shift += (0.10 if local_side == 1 else -0.10)

            # Anchor with pre-round baseline
            pre_prob = float(live_row.get("pre_round_prob", ally_mb_prob))
            combined = pre_prob + shift

            # Blend 40% Model B + 60% dynamic combat shift
            final_prob = 0.4 * ally_mb_prob + 0.6 * combined
            return max(0.01, min(0.99, float(final_prob)))
        except Exception as e:
            log.warning(f"Model B prediction failed: {e}")
            return 0.5


# ══════════════════════════════════════════════════════════════════════════════
# Log file watcher
# ══════════════════════════════════════════════════════════════════════════════

class LogWatcher:
    """
    Watches the most recently modified JSON file in raw_matchs_data/ or standard Documents log directory.
    """

    def __init__(self, watch_dir: Path):
        self.watch_dir    = watch_dir
        self.current_file: Path | None = None
        self.last_mtime   = 0.0
        self.start_time   = time.time()

    def get_latest_json(self) -> Path | None:
        candidates = []
        
        # Check primary project watch_dir (raw_matchs_data/valorant_game_events.json) and Documents fallbacks
        home = Path.home()
        possible_doc_files = [
            self.watch_dir / "valorant_game_events.json",
            self.watch_dir / "valorant_round_data.json",
            home / "OneDrive" / "Documents" / "valorant_game_events.json",
            home / "Documents" / "valorant_game_events.json",
            home / "OneDrive" / "Documents" / "valorant_round_data.json",
            home / "Documents" / "valorant_round_data.json",
        ]
        now = time.time()
        for p in possible_doc_files:
            if p.exists():
                try:
                    # Modified within last 30 minutes = active match stream
                    if p.stat().st_mtime >= (now - 1800):
                        candidates.append(p)
                except Exception:
                    pass

        # Check watch_dir (raw_matchs_data)
        try:
            for p in self.watch_dir.glob("*.json"):
                if not p.name.startswith("_") and not p.name.lower().startswith("test"):
                    if p.stat().st_mtime >= (now - 1800):
                        candidates.append(p)
        except Exception:
            pass

        if not candidates:
            return None

        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]

    def poll(self) -> tuple[list[dict], bool]:
        latest = self.get_latest_json()
        if not latest:
            return [], False

        try:
            mtime = latest.stat().st_mtime
        except Exception:
            return [], False

        if latest == self.current_file and mtime == self.last_mtime:
            return [], False

        is_new_file = (latest != self.current_file)
        if is_new_file:
            log.info(f"📂 Watching match file: {latest.name}")

        try:
            with open(latest, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content:
                return [], is_new_file
            events = self._try_parse_array(content)
            if events:
                self.current_file = latest
                self.last_mtime   = mtime
            return events, is_new_file
        except Exception as e:
            log.debug(f"Poll read error: {e}")
            return [], is_new_file

    def _try_parse_array(self, text: str) -> list[dict]:
        text = text.strip()
        if not text.startswith("["):
            return []
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            try:
                partial = text.rstrip(",").rstrip() + "]"
                return json.loads(partial)
            except Exception:
                return []


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket server & State global
# ══════════════════════════════════════════════════════════════════════════════

connected_clients: set = set()
global_state = RoundState()
match_history = MatchHistory()
last_match_end_payload: dict | None = None
last_match_report_payload: dict | None = None

async def ws_handler(websocket):
    connected_clients.add(websocket)
    log.info(f"🔗 Overlay connected ({len(connected_clients)} clients)")
    try:
        # Initial connection message
        await websocket.send(json.dumps({
            "type": "connected",
            "message": "Valorant predictor ready"
        }))

        # If a match is already active, send current state snapshot immediately
        if global_state.round_number >= 1 or global_state.map_name:
            await websocket.send(json.dumps({
                "type": "match_start"
            }))
            await websocket.send(json.dumps({
                "type"      : "pre_round",
                "round"     : max(1, global_state.round_number),
                "map"       : global_state.map_name,
                "side"      : global_state.local_side or "attack",
                "score_won" : global_state.score_won,
                "score_lost": global_state.score_lost,
                "prob"      : round(global_state.pre_round_prob * 100, 1),
                "buy_recommendation": global_state.last_buy_rec,
            }))
        elif last_match_end_payload:
            # Send latest match end report if overlay connected right after match ended
            await websocket.send(json.dumps(last_match_end_payload))
            if last_match_report_payload:
                await websocket.send(json.dumps(last_match_report_payload))

        async for _ in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        log.info(f"🔌 Overlay disconnected ({len(connected_clients)} clients)")


async def broadcast(message: dict):
    if not connected_clients:
        return
    payload = json.dumps(message)
    dead = set()
    for client in list(connected_clients):
        try:
            await client.send(payload)
        except Exception:
            dead.add(client)
    connected_clients.difference_update(dead)


# ══════════════════════════════════════════════════════════════════════════════
# Main game loop
# ══════════════════════════════════════════════════════════════════════════════

async def game_loop(predictor: Predictor, watcher: LogWatcher, buy_advisor: BuyAdvisor):
    state      = global_state
    history    = match_history
    last_phase = ""
    processed  = 0
    last_comp_sig = ""

    log.info(f"👁  Watching: {watcher.watch_dir}")
    log.info(f"🌐 WebSocket: ws://{WS_HOST}:{WS_PORT}")
    log.info("Waiting for a match to start...")

    while True:
        await asyncio.sleep(POLL_INTERVAL)

        events, is_new_file = watcher.poll()

        if is_new_file or (events and len(events) < processed):
            state.reset()
            processed  = 0
            last_phase = ""

        if not events or len(events) <= processed:
            continue

        new_events = events[processed:]
        processed  = len(events)   # Mark all current events as processed

        for item in new_events:
            event_type = item.get("type")

            # ── Info event ─────────────────────────────────────────────────
            if event_type == "info":
                data_block = item.get("data", {})
                mi = data_block.get("match_info", {})
                state.update_from_info(data_block)

                # Broadcast team composition + ranks whenever rosters change
                comp = state.build_team_comp()
                if comp and (comp["allies"] or comp["enemies"]):
                    sig = json.dumps(comp, sort_keys=True)
                    if sig != last_comp_sig:
                        last_comp_sig = sig
                        await broadcast({"type": "team_comp", **comp})

                if not mi:
                    continue

                phase = mi.get("round_phase")

                if phase and phase != last_phase:
                    last_phase = phase

                    if phase in ("shopping", "start", "buy") or (phase == "combat" and not state.pre_snap):
                        # FIRST: finalize the PREVIOUS round now that scores are updated
                        # (Overwolf updates the score between rounds, not at round end)
                        if history._current_round:
                            prev_won = state.score_won > state.prev_score_won
                            pending_fp = history._current_round.pop("final_prob_pending", None)
                            history.record_round_end(
                                round_num=history._current_round.get("round_number", 0),
                                score_won=state.score_won,
                                score_lost=state.score_lost,
                                won=prev_won,
                                final_prob=pending_fp if pending_fp is not None else state.live_prob,
                                round_report=parse_round_report(state.last_round_report),
                            )
                            # Update loss streaks from the last round's result
                            if prev_won:
                                state.ally_streak, state.enemy_streak = 0, state.enemy_streak + 1
                            else:
                                state.ally_streak, state.enemy_streak = state.ally_streak + 1, 0

                        # Carry over the plant flag for next-round economy projection
                        state.last_round_planted = state.planted_this_round
                        state.planted_this_round = False
                        state.capture_pre_round_snapshot()
                        prob = predictor.predict_pre_round(state.pre_snap)
                        state.pre_round_prob = prob
                        state.live_prob      = prob

                        # Snapshot current scores for detecting next round's result
                        state.prev_score_won  = state.score_won
                        state.prev_score_lost = state.score_lost

                        ally_players, enemy_players = state.get_scoreboard()
                        ally_moneys  = [max(0, int(p.get("money") or 0)) for p in ally_players]
                        enemy_moneys = [max(0, int(p.get("money") or 0)) for p in enemy_players]
                        ally_money  = sum(ally_moneys)
                        enemy_money = sum(enemy_moneys)

                        # Run buy recommendation engine
                        buy_rec = buy_advisor.recommend(
                            pre_snap=state.pre_snap,
                            ally_moneys=ally_moneys,
                            enemy_moneys=enemy_moneys,
                            local_side=state.local_side or "attack",
                            round_number=max(1, state.round_number),
                            score_won=state.score_won,
                            score_lost=state.score_lost,
                            ally_streak=state.ally_streak,
                            enemy_streak=state.enemy_streak,
                            plant_last=state.last_round_planted,
                            last_buy_rec=state.last_buy_rec,
                        )
                        state.last_buy_rec = buy_rec

                        # Record for post-match report
                        history.on_shopping_phase(
                            round_num=max(1, state.round_number),
                            side=state.local_side or "attack",
                            score_won=state.score_won,
                            score_lost=state.score_lost,
                            pre_prob=prob,
                            ally_money=ally_money,
                            enemy_money=enemy_money,
                            buy_rec=buy_rec,
                        )

                        log.info(
                            f"🎯 Round {max(1, state.round_number)} | {state.map_name or 'Valorant'} | "
                            f"Pre-round: {prob*100:.1f}% allies (Score {state.score_won}-{state.score_lost}) | "
                            f"Buy: {buy_rec['recommendation'].upper()}"
                        )
                        await broadcast({
                            "type"      : "pre_round",
                            "round"     : max(1, state.round_number),
                            "map"       : state.map_name,
                            "side"      : state.local_side or "attack",
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                            "prob"      : round(prob * 100, 1),
                            "buy_recommendation": buy_rec,
                        })

                    elif phase == "end":
                        # Round-report (damage/headshots/etc.) for the local player
                        summary = parse_round_report(state.last_round_report)

                        # Stash the end-of-round live probability; the round itself
                        # is finalized at the next shopping phase when the score has
                        # actually been updated by the stream.
                        if history._current_round:
                            history._current_round["final_prob_pending"] = state.live_prob

                        # Just broadcast round_end — actual win/loss is determined
                        # at the start of the next round when scores are updated
                        await broadcast({
                            "type"      : "round_end",
                            "round"     : max(1, state.round_number),
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                            "summary"   : summary,
                        })

                    elif phase in ("game_end", "match_end"):
                        outcome = state.raw.get("match_outcome", "")
                        # A mid-match "game_end" phase (practice/range or a glitchy
                        # stream) fires with an incomplete score — ignore it so we
                        # don't wipe live state or generate a bogus report.
                        if (not outcome or outcome == "unknown") and max(state.score_won, state.score_lost) < 13:
                            log.info(f"⏭️ Ignoring non-final game_end ({state.score_won}-{state.score_lost})")
                            last_phase = phase
                            continue
                        if not outcome or outcome == "unknown":
                            if state.score_won > state.score_lost or state.score_won >= 13:
                                outcome = "victory"
                            elif state.score_lost > state.score_won or state.score_lost >= 13:
                                outcome = "defeat"
                            else:
                                outcome = "victory" if state.score_won >= state.score_lost else "defeat"
                        log.info(f"🏁 Match ended — {outcome} ({state.score_won}-{state.score_lost})")

                        # Finalize last round
                        if history._current_round:
                            last_won = state.score_won > state.prev_score_won
                            pending_fp = history._current_round.pop("final_prob_pending", None)
                            history.record_round_end(
                                round_num=history._current_round.get("round_number", 0),
                                score_won=state.score_won,
                                score_lost=state.score_lost,
                                won=last_won,
                                final_prob=pending_fp if pending_fp is not None else state.live_prob,
                                round_report=parse_round_report(state.last_round_report),
                            )

                        report = history.generate_report(
                            map_name=state.map_name or "Valorant",
                            outcome=outcome,
                            final_score_won=state.score_won,
                            final_score_lost=state.score_lost,
                            local_agent=state.local_agent,
                            local_player_name=state.local_player_name,
                            team_comp=state.build_team_comp() or None,
                        )
                        global last_match_end_payload, last_match_report_payload
                        last_match_end_payload = {
                            "type"       : "match_end",
                            "outcome"    : outcome,
                            "score_won"  : state.score_won,
                            "score_lost" : state.score_lost,
                        }
                        last_match_report_payload = report

                        await broadcast(last_match_end_payload)
                        if report:
                            log.info(f"📊 Post-match overlay summary sent: {len(report.get('rounds', []))} rounds")
                            await broadcast(report)

                        state.reset()
                        history.reset()
                        last_phase = ""

            # ── Event: kill_feed, spike, match_start ───────────────────────
            elif event_type == "event":
                for ev in item.get("data", {}).get("events", []):
                    name = ev.get("name")

                    if name == "planted_location":
                        site = ev.get("data", "") or ""
                        carrier = ""
                        for i in range(10):
                            sb = state.raw.get(f"scoreboard_{i}")
                            if not sb:
                                continue
                            try:
                                p = json.loads(sb) if isinstance(sb, str) else sb
                            except Exception:
                                continue
                            if p.get("spike") is True and p.get("name"):
                                carrier = str(p["name"]).split("#")[0].strip()
                                break
                        state.on_spike_planted(site, carrier)
                        log.info(f"💣 Spike planted on {site or '?'}{' by ' + carrier if carrier else ''}")
                        await broadcast({
                            "type": "spike_planted",
                            "site": site,
                            "carrier": carrier,
                        })

                    elif name in ("spike_defused", "spike_detonated"):
                        site = state.spike_site or ""
                        state.on_spike_cleared()
                        if name == "spike_defused":
                            log.info(f"🛡️ Spike defused on {site or '?'}")
                        else:
                            log.info(f"💥 Spike detonated on {site or '?'}")
                        await broadcast({"type": name, "site": site})

                    elif name == "kill_feed":
                        if not state.pre_snap:
                            state.capture_pre_round_snapshot()

                        kf_data = ev.get("data", {})
                        if isinstance(kf_data, str):
                            try:
                                kf_data = json.loads(kf_data)
                            except Exception:
                                continue

                        live_row = state.on_kill(kf_data)
                        if not live_row:
                            continue

                        prob = predictor.predict_live(live_row)
                        state.live_prob = prob

                        attacker    = kf_data.get("attacker", "?")
                        victim      = kf_data.get("victim", "?")
                        headshot    = kf_data.get("headshot", False)
                        is_ally_kill = (
                            (state.local_side == "attack"  and kf_data.get("is_attacker_teammate")) or
                            (state.local_side == "defense" and not kf_data.get("is_attacker_teammate"))
                        )
                        history.on_kill(
                            kf_data=kf_data,
                            live_prob=prob,
                            local_player_name=state.local_player_name,
                            is_ally_kill=is_ally_kill,
                            att_alive=state.att_alive,
                            def_alive=state.def_alive,
                        )

                        log.info(
                            f"  Kill {state.kill_index}: {attacker} → {victim}"
                            f"{'  💥 HS' if headshot else ''}  |  "
                            f"{state.att_alive}v{state.def_alive}  |  "
                            f"Live: {prob*100:.1f}%"
                        )
                        await broadcast({
                            "type"                : "live_update",
                            "round"               : max(1, state.round_number),
                            "kill_index"          : state.kill_index,
                            "attacker"            : attacker,
                            "victim"              : victim,
                            "headshot"            : headshot,
                            "att_alive"           : state.att_alive,
                            "def_alive"           : state.def_alive,
                            "spike_planted"       : state.spike_planted,
                            "pre_round_prob"      : round(state.pre_round_prob * 100, 1),
                            "live_prob"           : round(prob * 100, 1),
                            "is_attacker_teammate": is_ally_kill,
                        })

                    elif name == "match_start":
                        log.info("🎮 Match started!")
                        snapshot = state.build_team_comp()
                        state.reset(keep_side=True)
                        state.team_comp_snapshot = snapshot
                        history.reset()
                        last_phase = ""
                        await broadcast({"type": "match_start"})


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def process_request(connection, request):
    conn = request.headers.get("Connection", "")
    if "Upgrade" in conn or "upgrade" in conn:
        request.headers["Connection"] = "Upgrade"
    return None


async def main():
    log.info("=" * 50)
    log.info("  Valorant Win Predictor — Backend Server")
    log.info("=" * 50)

    model_a, model_b = load_models()
    predictor = Predictor(model_a, model_b)
    advisor   = BuyAdvisor(model_a, A_FEATURES)
    log.info("💰 Buy recommendation engine initialized")

    watch_dir = LOG_WATCH_DIR
    if not watch_dir.exists():
        log.warning(f"Watch directory not found: {watch_dir}")
        watch_dir.mkdir(parents=True, exist_ok=True)

    watcher = LogWatcher(watch_dir)

    async with websockets.serve(ws_handler, WS_HOST, WS_PORT, process_request=process_request):
        log.info(f"WebSocket server running on ws://{WS_HOST}:{WS_PORT}")
        await game_loop(predictor, watcher, advisor)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped.")
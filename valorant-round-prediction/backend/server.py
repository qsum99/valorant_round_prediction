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
        self.round_number:    int   = 0
        self.map_name:        str   = ""
        if not keep_side:
            self.local_side:  str   = ""
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
        self.pre_round_prob:  float = 0.5
        self.live_prob:       float = 0.5

    def update_from_info(self, mi: dict):
        for k, v in mi.items():
            if v is not None:
                self.raw[k] = v

        if "round_phase" in mi:
            self.phase = mi["round_phase"]

        if self.raw.get("round_number"):
            try:
                self.round_number = int(self.raw["round_number"])
            except (ValueError, TypeError):
                pass

        if self.raw.get("map"):
            self.map_name = self.raw["map"]

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

    def on_spike_planted(self):
        self.spike_planted = True


# ══════════════════════════════════════════════════════════════════════════════
# Match history accumulator (for post-match report)
# ══════════════════════════════════════════════════════════════════════════════

FORCE_BUY_THRESHOLD = 14000  # between eco and full-buy

class MatchHistory:
    """Accumulates per-round data during a match for the post-match report."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.rounds = []          # list of round dicts
        self._current_round = {}  # in-progress round data
        self._round_kills = 0
        self._round_deaths = 0

    def record_round_start(self, round_num, side, score_won, score_lost,
                           pre_prob, ally_money, enemy_money):
        """Called at shopping/buy phase."""
        if round_num <= 1 and score_won == 0 and score_lost == 0:
            buy_type = "pistol"
        elif ally_money < ECO_THRESHOLD:
            buy_type = "eco"
        elif ally_money < FORCE_BUY_THRESHOLD:
            buy_type = "force"
        else:
            buy_type = "full_buy"

        self._current_round = {
            "round": round_num,
            "side": side,
            "score_before": [score_won, score_lost],
            "pre_prob": round(pre_prob * 100, 1),
            "ally_money": ally_money,
            "enemy_money": enemy_money,
            "buy_type": buy_type,
        }
        self._round_kills = 0
        self._round_deaths = 0

    def record_kill(self, is_ally_kill):
        """Called on each kill_feed event."""
        if is_ally_kill:
            self._round_kills += 1
        else:
            self._round_deaths += 1

    def record_round_end(self, round_num, score_won, score_lost,
                         won: bool, final_prob: float):
        """Called at end phase. `won` is explicitly computed by caller."""
        if not self._current_round:
            return

        cr = self._current_round
        cr["score_after"] = [score_won, score_lost]
        cr["won"] = won
        cr["final_prob"] = round(final_prob * 100, 1)
        cr["kills"] = self._round_kills
        cr["deaths"] = self._round_deaths

        # Performance: did we beat or miss our expected probability?
        pre = cr["pre_prob"]
        if won and pre < 40:
            cr["performance"] = "overperformed"
        elif not won and pre > 60:
            cr["performance"] = "underperformed"
        else:
            cr["performance"] = "expected"

        # Probability swing: how much the outcome shifted from pre-round expectation
        if won:
            cr["prob_swing"] = round(100 - pre, 1)
        else:
            cr["prob_swing"] = round(-pre, 1)

        self.rounds.append(cr)
        self._current_round = {}

    def generate_report(self, map_name, outcome, final_score_won, final_score_lost):
        """Generates the full post-match report payload."""
        if not self.rounds:
            return None

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
                    reason = f"Won upset round (only {r['pre_prob']}% expected)"
                elif r["buy_type"] == "eco":
                    reason = "Won eco round"
                else:
                    reason = "Key round win"
            else:
                if r["pre_prob"] > 60:
                    reason = f"Lost favored round ({r['pre_prob']}% expected)"
                elif r["buy_type"] == "full_buy":
                    reason = "Lost full-buy round"
                else:
                    reason = "Key round loss"
            pivotal.append({
                "round": r["round"],
                "swing": round(abs(swing), 1),
                "reason": reason,
                "won": r["won"],
                "pre_prob": r["pre_prob"],
            })

        # --- Economy efficiency by buy type ---
        economy = {}
        for bt in ("pistol", "eco", "force", "full_buy"):
            bt_rounds = [r for r in self.rounds if r.get("buy_type") == bt]
            economy[bt] = {
                "played": len(bt_rounds),
                "won": sum(1 for r in bt_rounds if r.get("won")),
            }

        return {
            "type": "match_report",
            "map": map_name,
            "outcome": outcome,
            "final_score": [final_score_won, final_score_lost],
            "rounds": self.rounds,
            "pivotal_rounds": pivotal,
            "economy": economy,
        }


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
    Watches the most recently modified JSON file in the watch directory
    or standard Overwolf Documents log directory.
    """

    def __init__(self, watch_dir: Path):
        self.watch_dir    = watch_dir
        self.current_file: Path | None = None
        self.last_mtime   = 0.0
        self.start_time   = time.time()

    def get_latest_json(self) -> Path | None:
        candidates = []
        
        # Check standard Overwolf log locations in Documents (active match can be up to 30 mins old)
        home = Path.home()
        possible_doc_files = [
            self.watch_dir / "valorant_game_events.json",
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

        self.current_file = latest
        self.last_mtime   = mtime

        try:
            with open(latest, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content:
                return [], is_new_file
            events = self._try_parse_array(content)
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
            }))

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

async def game_loop(predictor: Predictor, watcher: LogWatcher):
    state      = global_state
    history    = match_history
    last_phase = ""
    processed  = 0

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
                mi = item.get("data", {}).get("match_info", {})
                if not mi:
                    continue

                state.update_from_info(mi)
                phase = mi.get("round_phase")

                if phase and phase != last_phase:
                    last_phase = phase

                    if phase in ("shopping", "start", "buy") or (phase == "combat" and not state.pre_snap):
                        state.capture_pre_round_snapshot()
                        prob = predictor.predict_pre_round(state.pre_snap)
                        state.pre_round_prob = prob
                        state.live_prob      = prob

                        # Snapshot previous scores for won detection at round end
                        state.prev_score_won  = state.score_won
                        state.prev_score_lost = state.score_lost

                        # Record for post-match report
                        ally_money  = state.pre_snap.get("att_money", 0) if state.local_side == "attack" else state.pre_snap.get("def_money", 0)
                        enemy_money = state.pre_snap.get("def_money", 0) if state.local_side == "attack" else state.pre_snap.get("att_money", 0)
                        history.record_round_start(
                            round_num=max(1, state.round_number),
                            side=state.local_side or "attack",
                            score_won=state.score_won,
                            score_lost=state.score_lost,
                            pre_prob=prob,
                            ally_money=ally_money,
                            enemy_money=enemy_money,
                        )

                        log.info(
                            f"🎯 Round {max(1, state.round_number)} | {state.map_name or 'Valorant'} | "
                            f"Pre-round: {prob*100:.1f}% allies (Score {state.score_won}-{state.score_lost})"
                        )
                        await broadcast({
                            "type"      : "pre_round",
                            "round"     : max(1, state.round_number),
                            "map"       : state.map_name,
                            "side"      : state.local_side or "attack",
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                            "prob"      : round(prob * 100, 1),
                        })

                    elif phase == "end":
                        # Determine winner from score change
                        won = state.score_won > state.prev_score_won
                        history.record_round_end(
                            round_num=max(1, state.round_number),
                            score_won=state.score_won,
                            score_lost=state.score_lost,
                            won=won,
                            final_prob=state.live_prob,
                        )
                        await broadcast({
                            "type"      : "round_end",
                            "round"     : max(1, state.round_number),
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                        })

                    elif phase == "game_end":
                        outcome = state.raw.get("match_outcome", "unknown")
                        log.info(f"🏁 Match ended — {outcome}")

                        # Generate and broadcast post-match report
                        report = history.generate_report(
                            map_name=state.map_name,
                            outcome=outcome,
                            final_score_won=state.score_won,
                            final_score_lost=state.score_lost,
                        )
                        await broadcast({
                            "type"      : "match_end",
                            "outcome"   : outcome,
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                        })
                        if report:
                            log.info(f"📊 Post-match report: {len(report['rounds'])} rounds, {len(report['pivotal_rounds'])} pivotal")
                            await broadcast(report)
                        state.reset()
                        history.reset()
                        last_phase = ""

            # ── Event: kill_feed, spike, match_start ───────────────────────
            elif event_type == "event":
                for ev in item.get("data", {}).get("events", []):
                    name = ev.get("name")

                    if name == "planted_location":
                        state.on_spike_planted()
                        log.info(f"💣 Spike planted on {ev.get('data', '?')}")
                        await broadcast({"type": "spike_planted", "site": ev.get("data", "")})

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
                        history.record_kill(is_ally_kill)

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
                        state.reset(keep_side=True)
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

    watch_dir = LOG_WATCH_DIR
    if not watch_dir.exists():
        log.warning(f"Watch directory not found: {watch_dir}")
        watch_dir.mkdir(parents=True, exist_ok=True)

    watcher = LogWatcher(watch_dir)

    async with websockets.serve(ws_handler, WS_HOST, WS_PORT, process_request=process_request):
        log.info(f"WebSocket server running on ws://{WS_HOST}:{WS_PORT}")
        await game_loop(predictor, watcher)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped.")
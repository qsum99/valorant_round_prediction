"""
server.py  —  Valorant Win Predictor Backend
=============================================
Reads overflowf JSON log events in real-time, builds round state,
runs Model A (pre-round) and Model B (live) predictions, and pushes
probability updates to the React overlay via WebSocket.

Architecture:
  overflowf → writes JSON log → server reads it → models predict → WebSocket → React overlay

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
from copy import deepcopy

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
WS_HOST        = "localhost"
WS_PORT        = 8765
MODELS_DIR     = Path(__file__).parent.parent / "models"
LOG_WATCH_DIR  = Path(__file__).parent.parent / "data" / "raw"
POLL_INTERVAL  = 0.5   # seconds between log file checks

# ── Feature definitions (must match training exactly) ────────────────────────
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
    "att_wiping", "def_wiping",
    "spike_planted", "kill_index", "kill_progress",
    "round_number", "local_team_side", "map",
]

# ── Encodings (must match training exactly) ───────────────────────────────────
MAP_ENCODING = {
    "Ascent": 0, "Bonsai": 1, "Canyon": 2, "Foxtrot": 3,
    "Jam": 4, "Juliett": 5, "Pitt": 6, "Plummet": 7, "Triad": 8,
}
SIDE_ENCODING = {"attack": 0, "defense": 1}
MAX_TEAM_MONEY = 45_000
PLAYER_COUNT   = 5


# ══════════════════════════════════════════════════════════════════════════════
# Model loader
# ══════════════════════════════════════════════════════════════════════════════

def load_models():
    model_a_path = MODELS_DIR / "model_a.pkl"
    model_b_path = MODELS_DIR / "model_b.pkl"

    if not model_a_path.exists() or not model_b_path.exists():
        raise FileNotFoundError(
            f"Models not found in {MODELS_DIR}. "
            "Run train.ipynb first to generate model_a.pkl and model_b.pkl"
        )

    with open(model_a_path, "rb") as f:
        model_a = pickle.load(f)
    with open(model_b_path, "rb") as f:
        model_b = pickle.load(f)

    log.info(f"✅ Models loaded from {MODELS_DIR}")
    return model_a, model_b


# ══════════════════════════════════════════════════════════════════════════════
# Round state tracker
# ══════════════════════════════════════════════════════════════════════════════

class RoundState:
    """
    Accumulates overflowf streaming events into a complete round state.
    Mirrors the same logic as extract_features.py but runs in real-time.
    """

    def __init__(self):
        self.reset()

    def reset(self):
        # overflowf accumulated state
        self.raw: dict = {}

        # Round-level state
        self.round_number:    int   = 0
        self.map_name:        str   = ""
        self.local_side:      str   = ""   # "attack" or "defense"
        self.phase:           str   = ""   # shopping / combat / end
        self.score_won:       int   = 0
        self.score_lost:      int   = 0

        # Pre-round snapshot (captured at shopping phase)
        self.pre_snap:        dict  = {}

        # Live combat state
        self.att_alive:       int   = PLAYER_COUNT
        self.def_alive:       int   = PLAYER_COUNT
        self.att_kills:       int   = 0
        self.def_kills:       int   = 0
        self.kill_index:      int   = 0
        self.spike_planted:   bool  = False

        # Predictions
        self.pre_round_prob:  float = 0.5
        self.live_prob:       float = 0.5

    def update_from_info(self, mi: dict):
        """Merge a match_info partial update into accumulated state."""
        for k, v in mi.items():
            if v is not None:
                self.raw[k] = v

        # Extract key fields
        if "round_phase" in mi:
            self.phase = mi["round_phase"]

        if self.raw.get("round_number"):
            try:
                self.raw_round = int(self.raw["round_number"])
                self.round_number = self.raw_round
            except (ValueError, TypeError):
                pass

        if self.raw.get("map"):
            self.map_name = self.raw["map"]

        if self.raw.get("team"):
            self.local_side = self.raw["team"]

        if self.raw.get("score"):
            try:
                sc = json.loads(self.raw["score"]) if isinstance(self.raw["score"], str) else self.raw["score"]
                self.score_won  = sc.get("won",  0)
                self.score_lost = sc.get("lost", 0)
            except Exception:
                pass

    def get_scoreboard(self):
        """Parse 10-player scoreboard from accumulated raw state."""
        boards = []
        for i in range(10):
            raw = self.raw.get(f"scoreboard_{i}")
            if raw:
                try:
                    p = json.loads(raw) if isinstance(raw, str) else raw
                    boards.append(p)
                except Exception:
                    pass
        if len(boards) < 10:
            return None, None

        allies  = [p for p in boards if p.get("teammate") is True]
        enemies = [p for p in boards if p.get("teammate") is False]

        if len(allies) != PLAYER_COUNT or len(enemies) != PLAYER_COUNT:
            return None, None

        if self.local_side == "attack":
            return allies, enemies   # att=allies, def=enemies
        elif self.local_side == "defense":
            return enemies, allies   # att=enemies, def=allies
        return None, None

    def capture_pre_round_snapshot(self):
        """Called when round_phase switches to 'shopping'."""
        att, dff = self.get_scoreboard()
        if att is None:
            return False

        att_money = min(sum(p.get("money", 0) for p in att), MAX_TEAM_MONEY)
        def_money = min(sum(p.get("money", 0) for p in dff), MAX_TEAM_MONEY)
        att_ults  = sum(1 for p in att if p.get("ult_max", 0) > 0 and p.get("ult_points", 0) >= p.get("ult_max", 1))
        def_ults  = sum(1 for p in dff if p.get("ult_max", 0) > 0 and p.get("ult_points", 0) >= p.get("ult_max", 1))

        self.pre_snap = {
            "att_money"      : att_money,
            "def_money"      : def_money,
            "economy_diff"   : att_money - def_money,
            "att_full_buy"   : int(att_money >= 20_000),
            "def_full_buy"   : int(def_money >= 20_000),
            "att_eco"        : int(att_money < 5_000),
            "def_eco"        : int(def_money < 5_000),
            "att_ults_ready" : att_ults,
            "def_ults_ready" : def_ults,
            "ult_adv"        : att_ults - def_ults,
            "score_won"      : self.score_won,
            "score_lost"     : self.score_lost,
            "score_diff"     : self.score_won - self.score_lost,
            "round_number"   : self.round_number,
            "local_team_side": SIDE_ENCODING.get(self.local_side, 0),
            "map"            : MAP_ENCODING.get(self.map_name, 0),
        }

        # Reset live combat counters for new round
        self.att_alive    = PLAYER_COUNT
        self.def_alive    = PLAYER_COUNT
        self.att_kills    = 0
        self.def_kills    = 0
        self.kill_index   = 0
        self.spike_planted = False

        return True

    def on_kill(self, kf: dict) -> dict:
        """
        Called on each kill_feed event during combat.
        Returns the live feature row for Model B.
        """
        if not self.pre_snap:
            return {}

        self.kill_index += 1

        # Determine if attacker is on attacking side
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

        alive_ratio = self.att_alive / (self.att_alive + self.def_alive + 1e-6)

        alive_diff = self.att_alive - self.def_alive
        return {
            **self.pre_snap,
            "att_alive"    : self.att_alive,
            "def_alive"    : self.def_alive,
            "alive_diff"   : alive_diff,
            "alive_ratio"  : round(alive_ratio, 4),
            "att_kills"    : self.att_kills,
            "def_kills"    : self.def_kills,
            "kill_diff"    : self.att_kills - self.def_kills,
            "att_wiping"   : int(alive_diff >= 2),
            "def_wiping"   : int(alive_diff <= -2),
            "spike_planted": int(self.spike_planted),
            "kill_index"   : self.kill_index,
            "kill_progress": round(self.kill_index / 9.0, 4),
        }

    def on_spike_planted(self):
        self.spike_planted = True

    def to_status_dict(self) -> dict:
        """Serialise current state for the WebSocket message."""
        return {
            "round_number"  : self.round_number,
            "map"           : self.map_name,
            "side"          : self.local_side,
            "phase"         : self.phase,
            "score_won"     : self.score_won,
            "score_lost"    : self.score_lost,
            "att_alive"     : self.att_alive,
            "def_alive"     : self.def_alive,
            "att_kills"     : self.att_kills,
            "def_kills"     : self.def_kills,
            "kill_index"    : self.kill_index,
            "spike_planted" : self.spike_planted,
            "pre_round_prob": round(self.pre_round_prob * 100, 1),
            "live_prob"     : round(self.live_prob * 100, 1),
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
            prob = self.model_a.predict_proba(x)[0][1]
            return float(prob)
        except Exception as e:
            log.warning(f"Model A prediction failed: {e}")
            return 0.5

    def predict_live(self, live_row: dict) -> float:
        try:
            x = pd.DataFrame([live_row])[B_FEATURES].astype(float)
            prob = self.model_b.predict_proba(x)[0][1]
            return float(prob)
        except Exception as e:
            log.warning(f"Model B prediction failed: {e}")
            return 0.5


# ══════════════════════════════════════════════════════════════════════════════
# Log file watcher
# ══════════════════════════════════════════════════════════════════════════════

class LogWatcher:
    """
    Watches the most recently modified JSON file in the watch directory.
    Tails new lines as overflowf appends them during a live game.
    """

    def __init__(self, watch_dir: Path):
        self.watch_dir   = watch_dir
        self.current_file: Path | None = None
        self.file_pos    = 0
        self.buffer      = ""

    def get_latest_json(self) -> Path | None:
        files = sorted(
            self.watch_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        return files[0] if files else None

    def poll(self) -> list[dict]:
        """Return list of new JSON events since last poll."""
        latest = self.get_latest_json()
        if not latest:
            return []

        # New file appeared — reset position
        if latest != self.current_file:
            log.info(f"📂 New match file: {latest.name}")
            self.current_file = latest
            self.file_pos     = 0
            self.buffer       = ""

        events = []
        try:
            with open(self.current_file, "r", encoding="utf-8") as f:
                f.seek(self.file_pos)
                new_data = f.read()
                self.file_pos = f.tell()

            if not new_data.strip():
                return []

            # overflowf writes the file as a JSON array — parse incrementally
            self.buffer += new_data
            events = self._try_parse_array(self.buffer)

        except Exception as e:
            log.debug(f"Poll error: {e}")

        return events

    def _try_parse_array(self, text: str) -> list[dict]:
        """Try to parse as a JSON array, return empty list if incomplete."""
        text = text.strip()
        if not text.startswith("["):
            return []
        try:
            # Try parsing the whole array
            return json.loads(text)
        except json.JSONDecodeError:
            # Array not yet complete (game still running) — try partial parse
            # Add a closing bracket to see how far we get
            try:
                partial = text.rstrip(",").rstrip() + "]"
                return json.loads(partial)
            except Exception:
                return []


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket server
# ══════════════════════════════════════════════════════════════════════════════

connected_clients: set = set()

async def ws_handler(websocket):
    connected_clients.add(websocket)
    log.info(f"🔗 Overlay connected ({len(connected_clients)} clients)")
    try:
        # Send current state immediately on connect
        await websocket.send(json.dumps({"type": "connected", "message": "Valorant predictor ready"}))
        async for _ in websocket:
            pass  # We don't expect messages from overlay, but keep connection alive
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        log.info(f"🔌 Overlay disconnected ({len(connected_clients)} clients)")


async def broadcast(message: dict):
    """Send a message to all connected overlay clients."""
    if not connected_clients:
        return
    payload = json.dumps(message)
    await asyncio.gather(
        *[client.send(payload) for client in connected_clients],
        return_exceptions=True
    )


# ══════════════════════════════════════════════════════════════════════════════
# Main game loop
# ══════════════════════════════════════════════════════════════════════════════

async def game_loop(predictor: Predictor, watcher: LogWatcher):
    """
    Main loop: poll log file → process new events → predict → broadcast.
    Runs every POLL_INTERVAL seconds.
    """
    state       = RoundState()
    last_phase  = ""
    processed   = 0   # how many events we've processed so far

    log.info(f"👁  Watching: {watcher.watch_dir}")
    log.info(f"🌐 WebSocket: ws://{WS_HOST}:{WS_PORT}")
    log.info("Waiting for a match to start...")

    while True:
        await asyncio.sleep(POLL_INTERVAL)

        events = watcher.poll()
        if not events or len(events) <= processed:
            continue

        new_events = events[processed:]
        processed  = len(events)

        for item in new_events:
            event_type = item.get("type")

            # ── Info event: update round state ─────────────────────────────
            if event_type == "info":
                mi = item.get("data", {}).get("match_info", {})
                if not mi:
                    continue

                state.update_from_info(mi)
                phase = mi.get("round_phase")

                if phase and phase != last_phase:
                    last_phase = phase

                    # ── Shopping phase → pre-round prediction ───────────────
                    if phase == "shopping" and state.round_number > 1:
                        ok = state.capture_pre_round_snapshot()
                        if ok and state.pre_snap:
                            prob = predictor.predict_pre_round(state.pre_snap)
                            state.pre_round_prob = prob
                            state.live_prob      = prob   # live starts at pre-round baseline

                            log.info(
                                f"🎯 Round {state.round_number} | {state.map_name} | "
                                f"Pre-round: {prob*100:.1f}% allies"
                            )
                            await broadcast({
                                "type"      : "pre_round",
                                "round"     : state.round_number,
                                "map"       : state.map_name,
                                "side"      : state.local_side,
                                "score_won" : state.score_won,
                                "score_lost": state.score_lost,
                                "prob"      : round(prob * 100, 1),
                            })

                    # ── End phase → round over ─────────────────────────────
                    elif phase == "end":
                        await broadcast({
                            "type"      : "round_end",
                            "round"     : state.round_number,
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                        })

                    # ── Game end ───────────────────────────────────────────
                    elif phase == "game_end":
                        outcome = state.raw.get("match_outcome", "unknown")
                        log.info(f"🏁 Match ended — {outcome}")
                        await broadcast({
                            "type"   : "match_end",
                            "outcome": outcome,
                            "score_won" : state.score_won,
                            "score_lost": state.score_lost,
                        })
                        state.reset()
                        processed = 0

            # ── Event: kill_feed, spike ────────────────────────────────────
            elif event_type == "event":
                for ev in item.get("data", {}).get("events", []):
                    name = ev.get("name")

                    if name == "planted_location":
                        state.on_spike_planted()
                        log.info(f"💣 Spike planted on {ev.get('data', '?')}")
                        await broadcast({"type": "spike_planted", "site": ev.get("data", "")})

                    elif name == "kill_feed" and state.phase == "combat" and state.pre_snap:
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

                        attacker = kf_data.get("attacker", "?")
                        victim   = kf_data.get("victim", "?")
                        headshot = kf_data.get("headshot", False)

                        log.info(
                            f"  Kill {state.kill_index}: {attacker} → {victim}"
                            f"{'  💥 HS' if headshot else ''}  |  "
                            f"{state.att_alive}v{state.def_alive}  |  "
                            f"Live: {prob*100:.1f}%"
                        )
                        await broadcast({
                            "type"          : "live_update",
                            "round"         : state.round_number,
                            "kill_index"    : state.kill_index,
                            "attacker"      : attacker,
                            "victim"        : victim,
                            "headshot"      : headshot,
                            "att_alive"     : state.att_alive,
                            "def_alive"     : state.def_alive,
                            "spike_planted" : state.spike_planted,
                            "pre_round_prob": round(state.pre_round_prob * 100, 1),
                            "live_prob"     : round(prob * 100, 1),
                        })

                    elif name == "match_start":
                        log.info("🎮 Match started!")
                        state.reset()
                        processed = 0
                        await broadcast({"type": "match_start"})


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("=" * 50)
    log.info("  Valorant Win Predictor — Backend Server")
    log.info("=" * 50)

    # Load models
    model_a, model_b = load_models()
    predictor = Predictor(model_a, model_b)

    # Setup log watcher
    watch_dir = LOG_WATCH_DIR
    if not watch_dir.exists():
        log.warning(f"Watch directory not found: {watch_dir}")
        log.warning("Creating it. Drop your .json files here.")
        watch_dir.mkdir(parents=True, exist_ok=True)

    watcher = LogWatcher(watch_dir)

    # Start WebSocket server + game loop concurrently
    async with websockets.serve(ws_handler, WS_HOST, WS_PORT):
        log.info(f"WebSocket server running on ws://{WS_HOST}:{WS_PORT}")
        await game_loop(predictor, watcher)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped.")

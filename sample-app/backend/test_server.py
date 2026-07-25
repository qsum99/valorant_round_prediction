"""
test_server.py
--------------
Simulates a live Valorant match by replaying match4.json through the backend.
Use this to test the server WITHOUT needing to play a real game.

Usage:
    # Terminal 1 — start backend
    python backend/server.py

    # Terminal 2 — run simulation
    python backend/test_server.py
"""

import asyncio
import json
import time
import websockets
from pathlib import Path


REPLAY_FILE = Path(__file__).parent.parent / "data" / "raw" / "match4.json"
WS_URL      = "ws://localhost:8765"
EVENT_DELAY = 0.05   # seconds between events (0.05 = 20x speed)


async def listen():
    """Print all messages received from the backend."""
    async with websockets.connect(WS_URL) as ws:
        print(f"[Listener] Connected to {WS_URL}")
        async for msg in ws:
            data = json.loads(msg)
            msg_type = data.get("type", "?")

            if msg_type == "pre_round":
                print(f"\n🎯 Round {data['round']} | {data['map']} | Side: {data['side']} | "
                      f"Score: {data['score_won']}-{data['score_lost']}")
                print(f"   Pre-round probability: {data['prob']}% allies win")

            elif msg_type == "live_update":
                hs = " 💥HS" if data.get("headshot") else ""
                spike = " 💣" if data.get("spike_planted") else ""
                print(f"   Kill {data['kill_index']}: {data['attacker']} → {data['victim']}{hs}{spike} | "
                      f"{data['att_alive']}v{data['def_alive']} | "
                      f"Live: {data['live_prob']}%")

            elif msg_type == "spike_planted":
                print(f"   💣 Spike planted on site {data.get('site', '?')}")

            elif msg_type == "round_end":
                print(f"\n  Round ended. Score: {data['score_won']}-{data['score_lost']}")

            elif msg_type == "match_end":
                print(f"\n🏁 Match ended — {data.get('outcome', '?')}")
                print(f"   Final score: {data['score_won']}-{data['score_lost']}")

            elif msg_type == "connected":
                print(f"[Server] {data['message']}")


async def replay():
    """Replay match4.json by writing it to the watch directory."""
    if not REPLAY_FILE.exists():
        print(f"Replay file not found: {REPLAY_FILE}")
        print("Make sure match4.json is in data/raw/")
        return

    print(f"[Replay] Loading {REPLAY_FILE.name}...")
    with open(REPLAY_FILE) as f:
        events = json.load(f)

    out_path = REPLAY_FILE.parent / "_test_replay.json"
    written  = []

    print(f"[Replay] Replaying {len(events)} events at {1/EVENT_DELAY:.0f}x speed...")
    print("[Replay] Watch the listener terminal for predictions!\n")

    for i, event in enumerate(events):
        written.append(event)
        # Write incrementally to simulate live overflowf output
        with open(out_path, "w") as f:
            json.dump(written, f)
        await asyncio.sleep(EVENT_DELAY)

    print(f"\n[Replay] Done. Wrote {len(written)} events to {out_path.name}")


async def main():
    print("Starting test — make sure backend/server.py is running first!\n")
    await asyncio.sleep(1)

    # Run listener and replay concurrently
    await asyncio.gather(
        listen(),
        replay(),
    )


if __name__ == "__main__":
    asyncio.run(main())

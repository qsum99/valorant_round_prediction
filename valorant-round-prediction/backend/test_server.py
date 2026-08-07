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
import sys
import websockets
from pathlib import Path

# Force UTF-8 for stdout on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# The source match data to replay
SOURCE_FILE = Path(__file__).parent.parent / "raw_matchs_data" / "match4.json"
# Where the replayed events are written (Documents dir so server picks it up as active match log)
OUTPUT_FILE = Path.home() / "Documents" / "valorant_game_events.json"
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
                print(f"\n[Pre-Round] Round {data['round']} | {data['map']} | Side: {data['side']} | "
                      f"Score: {data['score_won']}-{data['score_lost']}")
                print(f"   Probability: {data['prob']}% allies win")

            elif msg_type == "live_update":
                hs = " (HS)" if data.get("headshot") else ""
                spike = " [SPIKE]" if data.get("spike_planted") else ""
                print(f"   Kill {data['kill_index']}: {data['attacker']} -> {data['victim']}{hs}{spike} | "
                      f"{data['att_alive']}v{data['def_alive']} | "
                      f"Live: {data['live_prob']}%")

            elif msg_type == "spike_planted":
                print(f"   [SPIKE PLANTED] Site {data.get('site', '?')}")

            elif msg_type == "round_end":
                print(f"\n[Round End] Score: {data['score_won']}-{data['score_lost']}")

            elif msg_type == "match_end":
                print(f"\n[Match End] {data.get('outcome', '?')}")
                print(f"   Final score: {data['score_won']}-{data['score_lost']}")

            elif msg_type == "match_report":
                print(f"\n{'='*60}")
                print(f"  POST-MATCH REPORT")
                print(f"{'='*60}")
                print(f"  Map: {data.get('map', '?')} | Outcome: {data.get('outcome', '?')}")
                print(f"  Final Score: {data['final_score'][0]}-{data['final_score'][1]}")
                print(f"  Rounds played: {len(data['rounds'])}")
                print()
                print("  Round | Side    | Buy      | Pre%  | Won | Perf")
                print("  " + "-"*56)
                for r in data["rounds"]:
                    side_str = r["side"][:3].upper()
                    print(f"  R{r['round']:>2}   | {side_str:<7} | {r['buy_type']:<8} | "
                          f"{r['pre_prob']:>5.1f} | {'✓' if r['won'] else '✗'}   | {r.get('performance', '')}")
                print()
                print("  PIVOTAL ROUNDS:")
                for p in data["pivotal_rounds"]:
                    icon = "🟢" if p["won"] else "🔴"
                    print(f"    {icon} R{p['round']}: {p['reason']} (swing: {p['swing']}%)")
                print()
                print("  ECONOMY:")
                for bt, stats in data["economy"].items():
                    if stats["played"] > 0:
                        wr = stats["won"] / stats["played"] * 100
                        print(f"    {bt:<8}: {stats['won']}/{stats['played']} won ({wr:.0f}%)")
                print(f"{'='*60}")

            elif msg_type == "connected":
                print(f"[Server] {data['message']}")


async def replay():
    """Replay match4.json by writing events incrementally to _test_replay.json."""
    if not SOURCE_FILE.exists():
        print(f"Replay file not found: {SOURCE_FILE}")
        print("Make sure match4.json is in raw_matchs_data/")
        return

    print(f"[Replay] Loading {SOURCE_FILE.name}...")
    with open(SOURCE_FILE, encoding="utf-8") as f:
        events = json.load(f)

    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()

    await asyncio.sleep(2)

    written = []
    print(f"[Replay] Replaying {len(events)} events at {1/EVENT_DELAY:.0f}x speed...")
    print("[Replay] Watch the listener terminal for predictions!\n")

    for i, event in enumerate(events):
        written.append(event)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(written, f)
        await asyncio.sleep(EVENT_DELAY)

        if (i + 1) % 500 == 0:
            print(f"[Replay] {i + 1}/{len(events)} events written...")

    print(f"\n[Replay] Done. Wrote {len(written)} events to {OUTPUT_FILE.name}")


async def main():
    print("Starting test — make sure backend/server.py is running first!\n")
    await asyncio.sleep(1)

    await asyncio.gather(
        listen(),
        replay(),
    )


if __name__ == "__main__":
    asyncio.run(main())
"""
test_server.py
--------------
Simulates a live Valorant match by replaying match data through the backend.
Use this to test the server WITHOUT needing to play a real game.

Usage:
    # Terminal 1 — start backend
    python backend/server.py

    # Terminal 2 — run simulation (defeat match)
    python backend/test_server.py

    # Or replay a victory match
    python backend/test_server.py --file match13
"""

import asyncio
import argparse
import json
import sys
import websockets
from pathlib import Path

# Force UTF-8 for stdout on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Available test matches
MATCH_DIR   = Path(__file__).parent.parent / "raw_matchs_data"
OUTPUT_FILE = MATCH_DIR / "valorant_game_events.json"
WS_URL      = "ws://localhost:8765"
DEFAULT_MATCH = "match4"  # defeat (11-13)


async def listen():
    """Print all messages received from the backend."""
    try:
        async with websockets.connect(WS_URL) as ws:
            print(f"[Listener] Connected to {WS_URL}")
            async for msg in ws:
                data = json.loads(msg)
                msg_type = data.get("type", "?")

            if msg_type == "pre_round":
                print(f"\n[Pre-Round] Round {data['round']} | {data['map']} | Side: {data['side']} | "
                      f"Score: {data['score_won']}-{data['score_lost']}")
                print(f"   Probability: {data['prob']}% allies win")
                if "buy_recommendation" in data and data["buy_recommendation"]:
                    br = data["buy_recommendation"]
                    rec = br.get("recommendation", "").upper()
                    urg = br.get("urgency", "").upper()
                    print(f"   💰 Buy Rec: [{rec}] ({urg} urgency) — {br.get('reason', '')}")
                    if br.get("scenarios"):
                        sc = br["scenarios"]
                        sc_str = " | ".join([f"{k}: {v['this_round']}% (EV: {v['two_round_ev']}%)" for k, v in sc.items()])
                        print(f"      Scenarios: {sc_str}")

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
                if "report_file" in data:
                    print(f"   📄 HTML Report saved: {data['report_file']}")

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
    except (websockets.exceptions.ConnectionClosed, ConnectionResetError, OSError):
        pass


EVENT_DELAY = 0.05   # seconds between events (0.05 = 20x speed, default as previous)


async def replay(source_file: Path, fast: bool = False, delay: float = EVENT_DELAY):
    """Replay a match by writing events incrementally to valorant_game_events.json."""
    if not source_file.exists():
        print(f"Replay file not found: {source_file}")
        print(f"Available matches in {MATCH_DIR}:")
        for f in sorted(MATCH_DIR.glob("match*.json")):
            print(f"  {f.stem}")
        return

    print(f"[Replay] Loading {source_file.name}...")
    with open(source_file, encoding="utf-8") as f:
        events = json.load(f)

    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()

    await asyncio.sleep(2)

    written = []
    if fast:
        batch_size = 25
        print(f"[Replay] Replaying {len(events)} events fast in batches of {batch_size}...")
        print("[Replay] Watch the listener terminal for predictions!\n")

        for i, event in enumerate(events):
            written.append(event)
            if (i + 1) % batch_size == 0 or (i + 1) == len(events):
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(written, f)
                await asyncio.sleep(0.01)

            if (i + 1) % 1000 == 0:
                print(f"[Replay] {i + 1}/{len(events)} events written...")
    else:
        print(f"[Replay] Replaying {len(events)} events at {1/delay:.0f}x speed (delay={delay}s)...")
        print("[Replay] Watch the listener terminal for predictions!\n")

        for i, event in enumerate(events):
            written.append(event)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(written, f)
            await asyncio.sleep(delay)

            if (i + 1) % 500 == 0:
                print(f"[Replay] {i + 1}/{len(events)} events written...")

    print(f"\n[Replay] Done. Wrote {len(written)} events to {OUTPUT_FILE.name}")


async def main(match_name: str, fast: bool = False, delay: float = EVENT_DELAY):
    source_file = MATCH_DIR / f"{match_name}.json"
    print(f"Starting test with {match_name} — make sure backend/server.py is running first!\n")
    await asyncio.sleep(1)

    await asyncio.gather(
        listen(),
        replay(source_file, fast=fast, delay=delay),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay a Valorant match through the backend")
    parser.add_argument(
        "--file", default=DEFAULT_MATCH,
        help=f"Match file stem to replay (default: {DEFAULT_MATCH}). Examples: match4 (defeat), match13 (victory)"
    )
    parser.add_argument(
        "--fast", action="store_true", default=False,
        help="Fast replay mode in batches (for quick post-match report testing)"
    )
    parser.add_argument(
        "--delay", type=float, default=EVENT_DELAY,
        help=f"Seconds delay per event in normal mode (default: {EVENT_DELAY})"
    )
    args = parser.parse_args()
    asyncio.run(main(args.file, fast=args.fast, delay=args.delay))
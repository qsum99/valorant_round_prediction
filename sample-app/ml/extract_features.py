"""
extract_features.py
--------------------
Reads overflowf JSON logs and outputs two CSVs:

  pre_round.csv  — one row per round (snapshot at shopping phase)
                   → training data for Model A (pre-round predictor)

  live_round.csv — one row per kill event per round
                   → training data for Model B (live predictor)

Usage:
    python extract_features.py                        # processes data/raw/*.json
    python extract_features.py path/to/match.json    # single file
    python extract_features.py data/raw/ -o data/    # custom output dir
"""

import json
import csv
import sys
import glob
import argparse
from pathlib import Path

# Resolve raw_matchs_data relative to this script's location
_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_RAW = str(_SCRIPT_DIR / ".." / "raw_matchs_data")
_DEFAULT_OUT = _DEFAULT_RAW  # CSVs go into the same raw data folder


# ── Constants ────────────────────────────────────────────────────────────────
MAX_TEAM_MONEY = 45_000   # 5 players × 9000 cap
PLAYER_COUNT   = 5


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_json(val):
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return val or {}


def parse_scoreboard(snap):
    """Return (allies, enemies) player lists from a state snapshot."""
    boards = []
    for i in range(10):
        raw = snap.get(f"scoreboard_{i}")
        if raw:
            boards.append(parse_json(raw))

    if len(boards) < 10:
        return None, None

    allies  = [p for p in boards if p.get("teammate") is True]
    enemies = [p for p in boards if p.get("teammate") is False]

    if len(allies) != PLAYER_COUNT or len(enemies) != PLAYER_COUNT:
        return None, None

    return allies, enemies


def att_def(allies, enemies, my_side):
    """Map allies/enemies to attacker/defender based on local player's side."""
    if my_side == "attack":
        return allies, enemies
    elif my_side == "defense":
        return enemies, allies
    return None, None


def economy_features(att, dff):
    att_money = min(sum(p.get("money", 0) for p in att), MAX_TEAM_MONEY)
    def_money = min(sum(p.get("money", 0) for p in dff), MAX_TEAM_MONEY)
    return att_money, def_money, att_money - def_money


def alive_features(att, dff):
    att_alive = sum(1 for p in att if p.get("alive"))
    def_alive = sum(1 for p in dff if p.get("alive"))
    return att_alive, def_alive, att_alive - def_alive


def ult_features(att, dff):
    def ult_ready(p):
        mx = p.get("ult_max", 0)
        return mx > 0 and p.get("ult_points", 0) >= mx

    att_ults = sum(1 for p in att if ult_ready(p))
    def_ults = sum(1 for p in dff if ult_ready(p))
    return att_ults, def_ults


def score_features(snap):
    sc = parse_json(snap.get("score"))
    won  = sc.get("won",  0) if sc else 0
    lost = sc.get("lost", 0) if sc else 0
    return won, lost, won - lost


# ── Per-round winner detection ───────────────────────────────────────────────

def build_round_winners(end_snapshots):
    """
    Returns dict {round_number_str: 'allies'|'enemies'}
    Derived from score changes between consecutive end-phase snapshots.
    """
    winners = {}
    prev_won = prev_lost = 0

    for snap in end_snapshots:
        sc = parse_json(snap.get("score"))
        if not sc:
            continue
        won  = sc.get("won",  0)
        lost = sc.get("lost", 0)
        rnd  = str(snap.get("round_number", ""))

        if not rnd:
            prev_won, prev_lost = won, lost
            continue

        if won > prev_won:
            winners[rnd] = "allies"
        elif lost > prev_lost:
            winners[rnd] = "enemies"

        prev_won, prev_lost = won, lost

    return winners


# ── Main extraction ──────────────────────────────────────────────────────────

def extract_from_file(filepath):
    """
    Returns:
        pre_rows  — list of dicts (one per round)
        live_rows — list of dicts (one per kill event per round)
    """
    with open(filepath) as f:
        data = json.load(f)

    source = Path(filepath).name

    # ── Pass 1: build full streaming state + collect snapshots ────────────
    state          = {}
    shopping_snaps = {}   # round_number -> snap at shopping phase
    end_snaps      = []   # snap at end phase (for winner detection)
    round_spikes   = {}   # round_number -> True if spike was planted

    # Also track kill events with their round context
    kill_timeline  = []   # [{round, kill_number_in_round, kf_data, snap_at_kill}]
    kills_in_round = {}   # round -> count

    for item in data:
        if item["type"] == "info":
            mi = item.get("data", {}).get("match_info", {})
            for k, v in mi.items():
                if v is not None:
                    state[k] = v

            phase = mi.get("round_phase")
            if phase == "shopping":
                rnd = state.get("round_number")
                if rnd and rnd not in shopping_snaps:
                    shopping_snaps[rnd] = dict(state)

            elif phase == "end":
                end_snaps.append(dict(state))

            elif phase == "combat":
                # Reset kill counter for new round
                rnd = state.get("round_number")
                if rnd and rnd not in kills_in_round:
                    kills_in_round[rnd] = 0

        elif item["type"] == "event":
            for ev in item.get("data", {}).get("events", []):

                if ev["name"] == "planted_location":
                    rnd = state.get("round_number")
                    if rnd:
                        round_spikes[rnd] = True

                elif ev["name"] == "kill_feed":
                    rnd = state.get("round_number")
                    if rnd and state.get("round_phase") == "combat":
                        kills_in_round[rnd] = kills_in_round.get(rnd, 0) + 1
                        kill_timeline.append({
                            "round"          : rnd,
                            "kill_index"     : kills_in_round[rnd],
                            "kf"             : parse_json(ev["data"]) if isinstance(ev["data"], str) else ev["data"],
                            "state_snap"     : dict(state),
                        })

    # ── Pass 2: build winner map from end snapshots ───────────────────────
    round_winners = build_round_winners(end_snaps)

    match_id = state.get("match_id", "")

    # ── Pass 3: build pre_round rows (Model A) ────────────────────────────
    pre_rows = []
    for rnd, snap in shopping_snaps.items():
        allies, enemies = parse_scoreboard(snap)
        if allies is None:
            continue

        my_side = snap.get("team")
        att, dff = att_def(allies, enemies, my_side)
        if att is None:
            continue

        att_money, def_money, economy_diff = economy_features(att, dff)
        att_alive, def_alive, alive_diff   = alive_features(att, dff)
        att_ults, def_ults                  = ult_features(att, dff)
        score_won, score_lost, score_diff   = score_features(snap)

        pre_rows.append({
            "source_file"    : source,
            "match_id"       : match_id,
            "round_number"   : rnd,
            "map"            : snap.get("map", ""),
            "local_team_side": my_side,
            # Economy
            "att_money"      : att_money,
            "def_money"      : def_money,
            "economy_diff"   : economy_diff,
            # Ults
            "att_ults_ready" : att_ults,
            "def_ults_ready" : def_ults,
            # Score context
            "score_won"      : score_won,
            "score_lost"     : score_lost,
            "score_diff"     : score_diff,
            # Label
            "winner"         : round_winners.get(rnd, ""),
        })

    # ── Pass 4: build live_round rows (Model B) ───────────────────────────
    live_rows = []
    # Track running combat state per round
    round_att_alive  = {}
    round_def_alive  = {}
    round_att_kills  = {}
    round_def_kills  = {}
    round_spike_planted = {}

    for entry in kill_timeline:
        rnd   = entry["round"]
        kf    = entry["kf"]
        snap  = entry["state_snap"]
        kidx  = entry["kill_index"]

        my_side = snap.get("team")
        if not my_side:
            continue

        # Get pre-round snapshot for economy/ult baseline
        pre_snap = shopping_snaps.get(rnd)
        if not pre_snap:
            continue

        allies_pre, enemies_pre = parse_scoreboard(pre_snap)
        if allies_pre is None:
            continue

        att_pre, def_pre = att_def(allies_pre, enemies_pre, my_side)
        if att_pre is None:
            continue

        att_money, def_money, economy_diff = economy_features(att_pre, def_pre)
        att_ults, def_ults                  = ult_features(att_pre, def_pre)
        score_won, score_lost, score_diff   = score_features(snap)

        # Update live alive/kill counts from kill_feed
        is_att_kill = (
            (my_side == "attack"  and kf.get("is_attacker_teammate")) or
            (my_side == "defense" and not kf.get("is_attacker_teammate"))
        )

        if rnd not in round_att_alive:
            round_att_alive[rnd] = PLAYER_COUNT
            round_def_alive[rnd] = PLAYER_COUNT
            round_att_kills[rnd] = 0
            round_def_kills[rnd] = 0

        if is_att_kill:
            round_att_kills[rnd] += 1
            round_def_alive[rnd] = max(0, round_def_alive[rnd] - 1)
        else:
            round_def_kills[rnd] += 1
            round_att_alive[rnd] = max(0, round_att_alive[rnd] - 1)

        att_alive  = round_att_alive[rnd]
        def_alive  = round_def_alive[rnd]
        att_kills  = round_att_kills[rnd]
        def_kills  = round_def_kills[rnd]
        alive_diff = att_alive - def_alive
        kill_diff  = att_kills - def_kills

        # Spike planted status at this kill moment
        spike_planted = 1 if round_spikes.get(rnd) and kidx > 1 else 0

        live_rows.append({
            "source_file"    : source,
            "match_id"       : match_id,
            "round_number"   : rnd,
            "kill_index"     : kidx,
            "map"            : snap.get("map", ""),
            "local_team_side": my_side,
            # Pre-round context (fixed for the round)
            "att_money"      : att_money,
            "def_money"      : def_money,
            "economy_diff"   : economy_diff,
            "att_ults_ready" : att_ults,
            "def_ults_ready" : def_ults,
            "score_won"      : score_won,
            "score_lost"     : score_lost,
            "score_diff"     : score_diff,
            # Live combat state (updates each kill)
            "att_alive"      : att_alive,
            "def_alive"      : def_alive,
            "alive_diff"     : alive_diff,
            "att_kills"      : att_kills,
            "def_kills"      : def_kills,
            "kill_diff"      : kill_diff,
            "spike_planted"  : spike_planted,
            # Label
            "winner"         : round_winners.get(rnd, ""),
        })

    return pre_rows, live_rows


# ── CSV writer ───────────────────────────────────────────────────────────────

def write_csv(rows, path, mode="w"):
    if not rows:
        return
    with open(path, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if mode == "w":
            writer.writeheader()
        writer.writerows(rows)


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="*", help="JSON files or directories (default: raw_matchs_data/)")
    parser.add_argument("-o", "--output-dir", default=_DEFAULT_OUT, help="Output directory for CSVs")
    args = parser.parse_args()

    # Resolve input files
    inputs = args.inputs or [_DEFAULT_RAW]
    paths  = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            paths.extend(sorted(
                f for f in p.glob("*.json") if not f.name.startswith(".")
            ))
        elif p.is_file():
            paths.append(p)
        else:
            for g in glob.glob(inp):
                paths.append(Path(g))

    if not paths:
        print("No JSON files found.")
        return

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    pre_path  = out_dir / "pre_round.csv"
    live_path = out_dir / "live_round.csv"

    all_pre  = []
    all_live = []

    for path in paths:
        try:
            pre, live = extract_from_file(path)
            print(f"  {path.name}: {len(pre)} rounds, {len(live)} kill-events")
            all_pre.extend(pre)
            all_live.extend(live)
        except Exception as e:
            print(f"  {path.name}: ERROR — {e}")

    write_csv(all_pre,  pre_path)
    write_csv(all_live, live_path)

    labeled_pre  = sum(1 for r in all_pre  if r["winner"])
    labeled_live = sum(1 for r in all_live if r["winner"])

    print(f"\npre_round.csv  -> {len(all_pre)} rows  ({labeled_pre} labeled)")
    print(f"live_round.csv -> {len(all_live)} rows ({labeled_live} labeled)")
    print(f"\nSaved to {out_dir}/")


if __name__ == "__main__":
    main()
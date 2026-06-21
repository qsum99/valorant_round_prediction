import json
import csv
import os
import sys

# ──────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'raw_matchs_data')
OUTPUT_FILE = os.path.join(RAW_DATA_DIR, 'round_data_aggregated.csv')
PROCESSED_MANIFEST = os.path.join(RAW_DATA_DIR, '.processed_files.json')

# CSV column order
CSV_COLUMNS = [
    'match_id', 'source_file', 'round_number', 'map', 'local_team_side',
    'first_kill_team', 'winner', 'score_won', 'score_lost', 'hs_stats',
    'att_money', 'def_money', 'att_kills', 'def_kills',
    'att_alive', 'def_alive', 'att_ults_ready', 'def_ults_ready',
    'att_hs_kills', 'def_hs_kills'
]

# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────

def load_processed_manifest():
    """Load the list of already-processed JSON filenames."""
    if os.path.exists(PROCESSED_MANIFEST):
        try:
            with open(PROCESSED_MANIFEST, 'r') as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_processed_manifest(processed_set):
    """Persist the set of processed filenames."""
    with open(PROCESSED_MANIFEST, 'w') as f:
        json.dump(sorted(processed_set), f, indent=2)


def get_json_files(directory):
    """Return sorted list of .json filenames in the given directory."""
    if not os.path.isdir(directory):
        print(f"Error: Directory '{directory}' not found.")
        return []
    files = [
        f for f in os.listdir(directory)
        if f.lower().endswith('.json') and not f.startswith('.')
    ]
    return sorted(files)


def load_data(filepath):
    """Load JSON data from a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"  Error loading {filepath}: {e}")
        return []


# ──────────────────────────────────────────────────────────
# Core extraction logic (unchanged from original)
# ──────────────────────────────────────────────────────────

def extract_round_data(events, source_filename):
    """
    Parses a list of game events to extract round-by-round data.
    Returns a list of dicts (one per completed round).
    """
    rounds = []

    # Global Match State (persists across rounds within one file)
    player_persistent_stats = {}

    # Current Round State
    current_round = {}
    round_active = False

    previous_score = {"won": 0, "lost": 0}
    current_match_id = None

    player_team_map = {}
    local_team_side = None

    match_cumulative_hs = {'allies': 0, 'enemies': 0}

    print(f"  Processing {len(events)} events from '{source_filename}'...")

    for event in events:
        if 'data' not in event:
            continue

        data = event['data']

        # --- Match Info Parsing ---
        if 'match_info' in data:
            mi = data['match_info']

            if 'match_id' in mi and mi['match_id'] != current_match_id:
                current_match_id = mi['match_id']
                previous_score = {"won": 0, "lost": 0}
                player_persistent_stats = {}
                player_team_map = {}
                local_team_side = None
                match_cumulative_hs = {'allies': 0, 'enemies': 0}

            # Track Local Team Side
            if 'team' in mi and mi['team']:
                local_team_side = mi['team']
                if round_active:
                    current_round['local_team_side'] = local_team_side

            # --- Scoreboard Updates ---
            for key in mi:
                if key.startswith("scoreboard_"):
                    try:
                        sb_str = mi[key]
                        if not sb_str:
                            continue
                        sb = json.loads(sb_str)

                        pid = sb.get('player_id')
                        if not pid:
                            continue

                        side = "allies" if sb.get("teammate") else "enemies"
                        player_team_map[pid] = side

                        if pid not in player_persistent_stats:
                            player_persistent_stats[pid] = {'headshots': 0}

                        if round_active:
                            p_curr = current_round['player_snapshots'].get(pid, {})

                            curr_money = sb.get('money', 0)
                            if curr_money is not None:
                                p_curr['money'] = max(p_curr.get('money', 0), curr_money)

                            curr_kills = sb.get('kills', 0)
                            if curr_kills is not None:
                                p_curr['kills'] = curr_kills

                            curr_ult = sb.get('ult_points', 0)
                            max_ult = sb.get('ult_max', 1)
                            if curr_ult is not None:
                                p_curr['ult_ready'] = 1 if curr_ult >= max_ult else 0

                            p_curr['alive'] = 1 if sb.get('alive', False) else 0

                            current_round['player_snapshots'][pid] = p_curr

                    except Exception:
                        pass

            # --- Kill Feed (Headshot Tracking) ---
            if 'kill_feed' in mi:
                try:
                    kf = json.loads(mi['kill_feed'])

                    if kf.get('headshot'):
                        is_ally = kf.get('is_attacker_teammate')
                        killer_team = "allies" if is_ally else "enemies"

                        if 'stats_tracker' not in current_round:
                            current_round['stats_tracker'] = {'allies_hs': 0, 'enemies_hs': 0}

                        current_round['stats_tracker'][f"{killer_team}_hs"] += 1

                    # First Blood
                    if round_active and current_round.get("first_kill_team") is None:
                        current_round["first_kill_team"] = (
                            "allies" if kf.get("is_attacker_teammate") else "enemies"
                        )

                except Exception:
                    pass

            # --- Round Phase Triggers ---
            if 'round_phase' in mi:
                phase = mi['round_phase']

                if phase in ['shopping', 'game_start'] and not round_active:
                    round_active = True
                    current_round = {
                        "match_id": current_match_id,
                        "source_file": source_filename,
                        "round_number": len(rounds) + 1,
                        "map": current_round.get('map'),
                        "local_team_side": local_team_side,
                        "player_snapshots": {},
                        "stats_tracker": {'allies_hs': 0, 'enemies_hs': 0},
                        "first_kill_team": None,
                        "winner": None,
                        "score_won": 0,
                        "score_lost": 0
                    }
                    print(f"    [Round {current_round['round_number']}] Started ({local_team_side})")

            # Score Update (Round End)
            if 'score' in mi:
                try:
                    score_data = json.loads(mi['score'])
                    new_won = score_data.get('won', 0)
                    new_lost = score_data.get('lost', 0)

                    winner = None
                    if new_won > previous_score['won']:
                        winner = "allies"
                    elif new_lost > previous_score['lost']:
                        winner = "enemies"

                    if round_active and winner:
                        # Calculate Team Sides
                        if local_team_side == 'attack':
                            side_map = {'allies': 'att', 'enemies': 'def'}
                        elif local_team_side == 'defend':
                            side_map = {'allies': 'def', 'enemies': 'att'}
                        else:
                            side_map = {'allies': 'att', 'enemies': 'def'}

                        feats = {
                            'att_money': 0, 'def_money': 0,
                            'att_kills': 0, 'def_kills': 0,
                            'att_alive': 0, 'def_alive': 0,
                            'att_ults_ready': 0, 'def_ults_ready': 0,
                            'att_hs_kills': 0, 'def_hs_kills': 0
                        }

                        for pid, p in current_round['player_snapshots'].items():
                            team_role = player_team_map.get(pid)
                            if not team_role:
                                continue

                            prefix = side_map.get(team_role)

                            feats[f"{prefix}_money"] += p.get('money', 0)
                            feats[f"{prefix}_kills"] += p.get('kills', 0)
                            feats[f"{prefix}_alive"] += p.get('alive', 0)
                            feats[f"{prefix}_ults_ready"] += p.get('ult_ready', 0)

                        if 'hs_stats' not in current_round:
                            current_round['hs_stats'] = {'allies': 0, 'enemies': 0}

                        feats[f"{side_map['allies']}_hs_kills"] = match_cumulative_hs['allies']
                        feats[f"{side_map['enemies']}_hs_kills"] = match_cumulative_hs['enemies']

                        if 'stats_tracker' in current_round:
                            match_cumulative_hs['allies'] += current_round['stats_tracker']['allies_hs']
                            match_cumulative_hs['enemies'] += current_round['stats_tracker']['enemies_hs']

                        feat_row = current_round.copy()
                        if 'player_snapshots' in feat_row:
                            del feat_row['player_snapshots']
                        if 'stats_tracker' in feat_row:
                            del feat_row['stats_tracker']

                        feat_row.update(feats)

                        feat_row['winner'] = winner
                        feat_row['score_won'] = new_won
                        feat_row['score_lost'] = new_lost

                        rounds.append(feat_row)

                        round_active = False
                        current_round = {}

                    previous_score = {"won": new_won, "lost": new_lost}

                except Exception:
                    pass

    return rounds


# ──────────────────────────────────────────────────────────
# CSV I/O  (supports APPEND mode for new files)
# ──────────────────────────────────────────────────────────

def append_to_csv(rounds, csv_path):
    """Append round rows to the CSV. Create with header if file doesn't exist."""
    if not rounds:
        return

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    mode = 'a' if file_exists else 'w'
    with open(csv_path, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()
        writer.writerows(rounds)


def print_summary(rounds):
    """Pretty-print a summary table of extracted rounds."""
    if not rounds:
        return
    print(f"\n  {'Round':<6} | {'Winner':<8} | {'Att $':<8} | {'Def $':<8} | {'Att K':<6} | {'Def K':<6}")
    print("  " + "-" * 60)
    for r in rounds:
        print(
            f"  {r.get('round_number', '?'):<6} | "
            f"{r.get('winner', 'None'):<8} | "
            f"{r.get('att_money', 0):<8} | "
            f"{r.get('def_money', 0):<8} | "
            f"{r.get('att_kills', 0):<6} | "
            f"{r.get('def_kills', 0):<6}"
        )


# ──────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Valorant Round Data Extractor")
    print("  Raw data folder: " + os.path.abspath(RAW_DATA_DIR))
    print("=" * 60)

    # 1. Discover JSON files
    all_json = get_json_files(RAW_DATA_DIR)
    if not all_json:
        print("\nNo JSON files found in raw_matchs_data/. Add match JSON files and re-run.")
        sys.exit(0)

    # 2. Determine which files are new
    already_processed = load_processed_manifest()
    new_files = [f for f in all_json if f not in already_processed]

    if not new_files:
        print(f"\nAll {len(all_json)} JSON file(s) already processed. Nothing to do.")
        print("To reprocess everything, delete '.processed_files.json' and the CSV.")
        sys.exit(0)

    print(f"\nFound {len(all_json)} JSON file(s) total, {len(new_files)} new to process:")
    for nf in new_files:
        print(f"  + {nf}")

    # 3. Process each new file and append
    total_rounds = 0
    for filename in new_files:
        filepath = os.path.join(RAW_DATA_DIR, filename)
        print(f"\n--- Processing: {filename} ---")

        raw_data = load_data(filepath)
        if not raw_data:
            print(f"  Skipping (empty or invalid JSON).")
            continue

        rounds = extract_round_data(raw_data, source_filename=filename)
        if rounds:
            append_to_csv(rounds, OUTPUT_FILE)
            print_summary(rounds)
            total_rounds += len(rounds)
            print(f"  [OK] {len(rounds)} rounds appended to CSV")
        else:
            print(f"  No rounds extracted from {filename}.")

        # Mark as processed
        already_processed.add(filename)

    # 4. Save manifest
    save_processed_manifest(already_processed)

    print(f"\n{'=' * 60}")
    print(f"  Done! {total_rounds} total new rounds appended.")
    print(f"  CSV output: {os.path.abspath(OUTPUT_FILE)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

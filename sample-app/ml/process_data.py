import json
import json
import os

# Configuration
INPUT_FILE = 'fisrt_match - Copy.json'
OUTPUT_FILE = 'round_data_aggregated.csv'

def load_data(filepath):
    """Loads JSON data from the given filepath."""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        print("Please copy 'fisrt_match - Copy.json' from your Documents folder to this directory.")
        return []
        
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return []

def extract_round_data(events):
    """
    Parses a list of game events to extract round-by-round data.
    """
    rounds = []
    
    # Global Match State (Persists across rounds)
    # player_id -> { headshots: 0 }
    player_persistent_stats = {} 
    
    # Current Round State
    current_round = {}
    round_active = False
    
    previous_score = {"won": 0, "lost": 0}
    current_match_id = None
    
    # We need to explicitly track team mapping: player_id -> 'allies' or 'enemies'
    # And mapping 'allies'/'enemies' -> 'attack'/'defend'
    player_team_map = {} 
    local_team_side = None # 'attack' or 'defend'

    match_cumulative_hs = {'allies': 0, 'enemies': 0}
    
    print(f"Processing {len(events)} events...")

    for i, event in enumerate(events):
        if 'data' not in event:
            continue
            
        data = event['data']
        
        # --- 1. Match Info Parsing ---
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
                # "team" in match_info is the local player's side
                local_team_side = mi['team']
                # Update current round if active
                if round_active:
                    current_round['local_team_side'] = local_team_side

            # --- Scoreboard Updates (Player Stats) ---
            for key in mi:
                if key.startswith("scoreboard_"):
                    try:
                        sb_str = mi[key]
                        if not sb_str: continue
                        sb = json.loads(sb_str)
                        
                        pid = sb.get('player_id')
                        if not pid: continue
                        
                        # Update Team Logic
                        side = "allies" if sb.get("teammate") else "enemies"
                        player_team_map[pid] = side
                        
                        # Initialize persistent tracking
                        if pid not in player_persistent_stats:
                            player_persistent_stats[pid] = {'headshots': 0}

                        # If Round Active, capture Snapshot for the round start features
                        # We try to capture the 'best' snapshot available during the Shopping phase.
                        # Since we process strictly sequentially, valid updates during 'round_active' (which starts at shopping)
                        # will form our feature set. 
                        # To avoid capturing mid-round damage/deaths as "start" stats, we could only update 
                        # if phase == 'shopping'.
                        
                        phase = mi.get('round_phase', '') # Might not be in this specific event, need state tracking
                        # Simplified: distinct updates for "Start Stats" vs "End Results".
                        # For "Prediction Features", we want cumulative stats UP TO this round.
                        # Scoreboard 'kills' are cumulative.
                        
                        if round_active:
                            # Update the temporary player snapshot for this round
                            p_curr = current_round['player_snapshots'].get(pid, {})
                            
                            # Money: Max seen (Economy)
                            curr_money = sb.get('money', 0)
                            if curr_money is not None:
                                p_curr['money'] = max(p_curr.get('money', 0), curr_money)
                                
                            # Kills/Deaths: Min seen (Snapshot at start before new ones happen)
                            # Actually, if we are in shopping, kills shouldn't change.
                            # We take the value present.
                            curr_kills = sb.get('kills', 0)
                            if curr_kills is not None:
                                p_curr['kills'] = curr_kills
                                
                            # Ult Points
                            curr_ult = sb.get('ult_points', 0)
                            max_ult = sb.get('ult_max', 1)
                            if curr_ult is not None:
                                p_curr['ult_ready'] = 1 if curr_ult >= max_ult else 0
                                
                            # Alive
                            p_curr['alive'] = 1 if sb.get('alive', False) else 0

                            current_round['player_snapshots'][pid] = p_curr

                    except Exception as e:
                        pass

            # --- Kill Feed (Headshot Tracking) ---
            if 'kill_feed' in mi:
                try:
                    kf = json.loads(mi['kill_feed'])
                    # Kill feed events happen DURING combat.
                    # We update PERSISTENT stats.
                    # Be careful: These headshots should apply to the NEXT round's "historical" features.
                    # Or applied mid-round? 
                    # If we follow "Events Stream", kill feed happens after Shopping.
                    # So for Round N, the 'kill_feed' events update the stats for Round N+1 features.
                    
                    attacker_id = None # Need to find ID from name? 
                    # JSON kill_feed usually has names, not IDs directly? 
                    # "kill_feed": "{\"attacker\":\"RISHU9427\",\"victim\":\"...\"...}"
                    # We might need to map Name -> ID if possible, or just track by Name.
                    # Scoreboard has Name and ID.
                    
                    # For now, let's look for "is_headshot" and attribute it to... determining if it was ally/enemy
                    # The user wants "att_hs_kills" and "def_hs_kills".
                    # Kill feed has "is_attacker_teammate".
                    
                    if kf.get('headshot'):
                        # Attribute to team
                        is_ally = kf.get('is_attacker_teammate')
                        # We can track team-level headshots directly if player mapping is hard
                        killer_team = "allies" if is_ally else "enemies"
                        
                        # Add to persistent team tally
                        # But wait, local_player side flips (Attack/Defend). 
                        # We need to track by "Allies/Enemies" then map to Side at round export?
                        # Yes.
                        
                        # We'll tag it in the current round to increment the global counter later?
                        # Or just increment a global "Allies HS" / "Enemies HS" counter?
                        # Taking a global counter is risky if rosters shuffle, but safe for one match.
                        
                        if 'stats_tracker' not in current_round: 
                            current_round['stats_tracker'] = {'allies_hs': 0, 'enemies_hs': 0}
                        
                        current_round['stats_tracker'][f"{killer_team}_hs"] += 1
                        
                    # First Blood
                    if round_active and current_round.get("first_kill_team") is None:
                        current_round["first_kill_team"] = "allies" if kf.get("is_attacker_teammate") else "enemies"

                except:
                    pass

            # --- Round Phase Triggers ---
            if 'round_phase' in mi:
                phase = mi['round_phase']
                
                # Start Round
                if phase in ['shopping', 'game_start'] and not round_active:
                    round_active = True
                    current_round = {
                        "match_id": current_match_id,
                        "round_number": len(rounds) + 1,
                        "map": current_round.get('map'), # Preserve map
                        "local_team_side": local_team_side,
                        "player_snapshots": {}, # Captured at start
                        "stats_tracker": {'allies_hs': 0, 'enemies_hs': 0}, # Events this round
                        # Result Placeholders
                        "first_kill_team": None,
                        "winner": None,
                        "score_won": 0,
                        "score_lost": 0
                    }
                    print(f"  [Round {current_round['round_number']}] Started ({local_team_side})")

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
                        # --- AGGREGATION TIME ---
                        # We aggregate the SNAPSHOTS captured during shopping (mostly).
                        # Plus the cumulative HS counts (Previous Rounds + this round? No, features for *this* round use *previous* history).
                        # Wait, "att_kills" for Round N usually means "Returns for Round N prediction", so "Kills before Round N".
                        # Correct.
                        
                        # Calculate Team Sides
                        # local_team_side is for the CURRENT round.
                        # If local="attack", Allies=Attack, Enemies=Defend.
                        if local_team_side == 'attack':
                            side_map = {'allies': 'att', 'enemies': 'def'}
                        elif local_team_side == 'defend':
                            side_map = {'allies': 'def', 'enemies': 'att'}
                        else:
                            side_map = {'allies': 'att', 'enemies': 'def'} # Fallback

                        # Init features
                        feats = {
                            'att_money': 0, 'def_money': 0,
                            'att_kills': 0, 'def_kills': 0,
                            'att_alive': 0, 'def_alive': 0,
                            'att_ults_ready': 0, 'def_ults_ready': 0,
                            'att_hs_kills': 0, 'def_hs_kills': 0 # We need cumulative HS
                        }
                        
                        # 1. Aggregate from Player Snapshots (Scoreboard stats)
                        for pid, p in current_round['player_snapshots'].items():
                            team_role = player_team_map.get(pid) # allies/enemies
                            if not team_role: continue
                            
                            prefix = side_map.get(team_role) # att/def
                            
                            feats[f"{prefix}_money"] += p.get('money', 0)
                            feats[f"{prefix}_kills"] += p.get('kills', 0)
                            feats[f"{prefix}_alive"] += p.get('alive', 0)
                            feats[f"{prefix}_ults_ready"] += p.get('ult_ready', 0)
                            
                            # Add Headshots (Persistent tracking would be better per-player, 
                            # but simpler to track global team HS if we trust the sides don't swap mid-match? 
                            # Sides SWAP at round 13.
                            # So team_stats must be tracked by "Allies/Enemies".
                            # But Allies/Enemies SWAP sides.
                            # So "Allies" accumulate headshots. 
                            # In Round 1 (Allies=Defend), they get 5 HS.
                            # In Round 13 (Allies=Attack), they still have those 5 HS + new ones.
                            # So feats[f"{prefix}_hs_kills"] should come from a persistent "Allies Total HS" counter.
                            
                        # Update Persistent HS counters with *Last Round's* data?
                        # No, we need a persistent "Allies HS" counter that increments on events.
                        # We'll use global `total_allies_hs` variables?
                        # Need to store in Match State.
                        
                        # Hack: We define current round HS features as "Previous Total".
                        # Then we add this round's HS to the total for the next round.
                        
                        # Let's use `current_match_hs` state
                        if 'hs_stats' not in current_round: # Should not happen if correctly init
                             current_round['hs_stats'] = {'allies': 0, 'enemies': 0}
                             
                        # To retrieve correctly:
                        # We need a `match_cumulative_hs` dict outside the round loop
                        # But passed into function? No, `rounds` loop.
                        # I'll modify the outer scope.
                        pass # handled below

                        # --- HEADSHOT MAPPING ---
                        # Use match_cumulative_hs (snapshot from START of round) for features
                        # But wait, we want HS *up to* this round.
                        # `match_cumulative_hs` should track total HS completed.
                        # We apply the values to features.
                        
                        # Map Allies/Enemies to Att/Def using `side_map`
                        # Note: `match_cumulative_hs` keys are 'allies'/'enemies'.
                        # This assumes 'Allies' means the SAME TEAM throughout.
                        # If sides switch (Att->Def), the 'Allies' team is still 'Allies', just on a different side.
                        # So `allies_hs` should map to `side_map['allies']` => `att` or `def`.
                        
                        feats[f"{side_map['allies']}_hs_kills"] = match_cumulative_hs['allies']
                        feats[f"{side_map['enemies']}_hs_kills"] = match_cumulative_hs['enemies']
                        
                        # Now UPDATE the cumulative counters for the NEXT round
                        # using the stats_tracker from THIS round.
                        if 'stats_tracker' in current_round:
                            match_cumulative_hs['allies'] += current_round['stats_tracker']['allies_hs']
                            match_cumulative_hs['enemies'] += current_round['stats_tracker']['enemies_hs']

                        # Map the cumulative HS to features
                        
                        feat_row = current_round.copy()
                        if 'player_snapshots' in feat_row: del feat_row['player_snapshots']
                        if 'stats_tracker' in feat_row: del feat_row['stats_tracker']
                        
                        # Merge aggregates
                        feat_row.update(feats)
                        
                        # Add winner info
                        feat_row['winner'] = winner
                        feat_row['score_won'] = new_won
                        feat_row['score_lost'] = new_lost
                        
                        rounds.append(feat_row)
                        
                        round_active = False
                        current_round = {}

                    previous_score = {"won": new_won, "lost": new_lost}

                except Exception as e:
                    # print(f"Error: {e}")
                    pass
                    
    return rounds

import csv

def save_to_csv(rounds):
    if not rounds:
        print("No rounds extracted.")
        return

    try:
        keys = rounds[0].keys()
        with open(OUTPUT_FILE, 'w', newline='') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(rounds)
            
        print(f"\nSuccess! Saved {len(rounds)} rounds to '{OUTPUT_FILE}'")
        # Simple preview
        print(f"{'Round':<6} | {'Winner':<8} | {'Att $':<8} | {'Def $':<8} | {'Att K':<6} | {'Def K':<6}")
        print("-" * 60)
        for r in rounds:
            print(f"{r['round_number']:<6} | {r.get('winner', 'None'):<8} | {r.get('att_money', 0):<8} | {r.get('def_money', 0):<8} | {r.get('att_kills', 0):<6} | {r.get('def_kills', 0):<6}")

    except Exception as e:
        print(f"Error saving CSV: {e}")

if __name__ == "__main__":
    print("--- Valorant Round Data Extractor ---")
    raw_data = load_data(INPUT_FILE)
    if raw_data:
        processed_rounds = extract_round_data(raw_data)
        save_to_csv(processed_rounds)

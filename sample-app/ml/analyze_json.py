
import json
import os

FILE = 'fisrt_match - Copy.json'

def analyze():
    if not os.path.exists(FILE):
        print("File not found.")
        return

    with open(FILE, 'r') as f:
        data = json.load(f)

    print(f"Total events: {len(data)}")

    phases = []
    outcomes = []
    scores = []
    rounds = []

    for i, e in enumerate(data):
        if 'data' not in e: continue
        d = e['data']
        
        # Check match_info
        if 'match_info' in d:
            mi = d['match_info']
            
            if 'round_phase' in mi:
                phases.append((i, e.get('timestamp'), mi['round_phase']))
            
            if 'match_outcome' in mi:
                outcomes.append((i, e.get('timestamp'), mi['match_outcome']))
                
            if 'score' in mi:
                scores.append((i, e.get('timestamp'), mi['score']))
            
            if 'round_number' in mi:
                rounds.append((i, e.get('timestamp'), mi['round_number']))

    print("\n--- Round Phases ---")
    for p in phases:
        print(p)

    print("\n--- Match Outcomes ---")
    for o in outcomes:
        print(o)

    print("\n--- Scores ---")
    for s in scores:
        print(s)

    print("\n--- Round Numbers ---")
    for r in rounds:
        print(r)

if __name__ == "__main__":
    analyze()

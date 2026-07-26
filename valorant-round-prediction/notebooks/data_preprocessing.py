"""
data_preprocessing.py
---------------------
Complete data preprocessing pipeline for Valorant round prediction.

Reads:
    pre_round.csv   - Pre-round features (Model A training data)
    live_round.csv  - Live-round features (Model B training data)

Outputs (in ./processed/ directory):
    pre_round_train.csv   / pre_round_test.csv
    live_round_train.csv  / live_round_test.csv

Notes:
    - No feature scaling (XGBoost is tree-based, doesn't need it)
    - Map and side are label-encoded as integers, NOT scaled
    - Both raw features AND diff features are kept

Usage:
    python data_preprocessing.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import GroupShuffleSplit


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

DATA_DIR     = Path(__file__).resolve().parent
OUTPUT_DIR   = DATA_DIR / "processed"

# Maps to remove (too few samples or different game mode)
RARE_MAPS = ["HURM_Alley", "HURM_Yard", "Pitt", "Canyon"]

# Outlier thresholds for live-round data
MAX_KILL_INDEX = 10   # Normal 5v5 round can have at most 10 kills
MAX_ATT_KILLS  = 5    # Max 5 enemies to kill
MAX_DEF_KILLS  = 5    # Max 5 attackers to kill

# Columns to drop (identifiers, not features)
ID_COLUMNS = ["source_file"]

# Train/test split ratio
TEST_SIZE = 0.2
RANDOM_STATE = 42

# Target column
TARGET = "winner"


# ------------------------------------------------------------------------------
# Step 1: Load Data
# ------------------------------------------------------------------------------

def load_data():
    """Load both CSVs and return DataFrames."""
    pre_df  = pd.read_csv(DATA_DIR / "pre_round.csv")
    live_df = pd.read_csv(DATA_DIR / "live_round.csv")

    print("=" * 70)
    print("STEP 1: Load Data")
    print("=" * 70)
    print(f"  Pre-round:  {pre_df.shape[0]:,} rows x {pre_df.shape[1]} columns")
    print(f"  Live-round: {live_df.shape[0]:,} rows x {live_df.shape[1]} columns")

    return pre_df, live_df


# ------------------------------------------------------------------------------
# Step 2: Drop NaN Winners
# ------------------------------------------------------------------------------

def drop_nan_winners(pre_df, live_df):
    """Remove rows where the winner is unknown (NaN or empty string)."""
    pre_before  = len(pre_df)
    live_before = len(live_df)

    # Handle both NaN and empty string cases
    pre_df  = pre_df[pre_df[TARGET].notna() & (pre_df[TARGET] != "")].copy()
    live_df = live_df[live_df[TARGET].notna() & (live_df[TARGET] != "")].copy()

    print(f"\nSTEP 2: Drop NaN Winners")
    print(f"  Pre-round:  {pre_before:,} -> {len(pre_df):,} (removed {pre_before - len(pre_df):,})")
    print(f"  Live-round: {live_before:,} -> {len(live_df):,} (removed {live_before - len(live_df):,})")

    return pre_df, live_df


# ------------------------------------------------------------------------------
# Step 3: Remove Rare/Invalid Maps
# ------------------------------------------------------------------------------

def remove_rare_maps(pre_df, live_df):
    """Remove rows from maps with too few samples or invalid game modes."""
    pre_before  = len(pre_df)
    live_before = len(live_df)

    pre_df  = pre_df[~pre_df["map"].isin(RARE_MAPS)].copy()
    live_df = live_df[~live_df["map"].isin(RARE_MAPS)].copy()

    print(f"\nSTEP 3: Remove Rare/Invalid Maps ({', '.join(RARE_MAPS)})")
    print(f"  Pre-round:  {pre_before:,} -> {len(pre_df):,} (removed {pre_before - len(pre_df):,})")
    print(f"  Live-round: {live_before:,} -> {len(live_df):,} (removed {live_before - len(live_df):,})")
    print(f"  Remaining maps: {sorted(pre_df['map'].unique())}")

    return pre_df, live_df


# ------------------------------------------------------------------------------
# Step 4: Remove Outliers (Live-Round Only)
# ------------------------------------------------------------------------------

def remove_outliers(live_df):
    """Remove anomalous rows from live-round data."""
    before = len(live_df)

    mask = (
        (live_df["kill_index"] <= MAX_KILL_INDEX) &
        (live_df["att_kills"]  <= MAX_ATT_KILLS) &
        (live_df["def_kills"]  <= MAX_DEF_KILLS)
    )
    live_df = live_df[mask].copy()

    print(f"\nSTEP 4: Remove Outliers (Live-Round)")
    print(f"  Thresholds: kill_index <= {MAX_KILL_INDEX}, att_kills <= {MAX_ATT_KILLS}, def_kills <= {MAX_DEF_KILLS}")
    print(f"  Live-round: {before:,} -> {len(live_df):,} (removed {before - len(live_df):,})")

    return live_df


# ------------------------------------------------------------------------------
# Step 5: Encode Target Variable
# ------------------------------------------------------------------------------

def encode_target(pre_df, live_df):
    """Encode winner as binary: allies=1, enemies=0."""
    mapping = {"allies": 1, "enemies": 0}

    pre_df[TARGET]  = pre_df[TARGET].map(mapping)
    live_df[TARGET] = live_df[TARGET].map(mapping)

    print(f"\nSTEP 5: Encode Target Variable")
    print(f"  Mapping: {mapping}")
    print(f"  Pre-round  class balance: {pre_df[TARGET].value_counts().to_dict()}")
    print(f"  Live-round class balance: {live_df[TARGET].value_counts().to_dict()}")

    return pre_df, live_df


# ------------------------------------------------------------------------------
# Step 6: Encode Categorical Features (Label Encode, NO scaling)
# ------------------------------------------------------------------------------

def encode_categoricals(pre_df, live_df):
    """Label encode 'map' as integers, binary encode 'local_team_side'.
    
    These are categorical features -- they must NOT be scaled.
    XGBoost handles integer-encoded categoricals natively.
    """
    # Binary encode local_team_side: attack=1, defense=0
    side_map = {"attack": 1, "defense": 0}
    pre_df["local_team_side"]  = pre_df["local_team_side"].map(side_map)
    live_df["local_team_side"] = live_df["local_team_side"].map(side_map)

    # Label encode map (alphabetical order for consistency)
    all_maps = sorted(set(pre_df["map"].unique()) | set(live_df["map"].unique()))
    map_to_int = {m: i for i, m in enumerate(all_maps)}

    pre_df["map"]  = pre_df["map"].map(map_to_int)
    live_df["map"] = live_df["map"].map(map_to_int)

    print(f"\nSTEP 6: Encode Categorical Features (Label Encoding, no scaling)")
    print(f"  local_team_side: {side_map}")
    print(f"  map label encoding: {map_to_int}")

    return pre_df, live_df


# ------------------------------------------------------------------------------
# Step 7: Feature Engineering
# ------------------------------------------------------------------------------

# Economy thresholds (team total across 5 players)
FULL_BUY_THRESHOLD = 19500   # ~3900 per player (Vandal/Phantom + full armor + abilities)
ECO_THRESHOLD      = 10000   # ~2000 per player (pistol/eco round)

def engineer_features(pre_df, live_df):
    """Create derived features that add predictive signal.
    
    Common features (both datasets):
        att_full_buy  - 1 if attackers can full buy
        def_full_buy  - 1 if defenders can full buy
        att_eco       - 1 if attackers are on eco
        def_eco       - 1 if defenders are on eco
        ult_adv       - attacker ult advantage (att_ults_ready - def_ults_ready)
    
    Live-round only:
        alive_ratio   - fraction of alive players on attacker side (0.0 to 1.0)
        att_wiping    - 1 if attackers eliminated all defenders
        def_wiping    - 1 if defenders eliminated all attackers
        kill_progress - fraction of total kills so far (0.0 to 1.0)
    """
    # -- Common features (pre-round + live-round) --
    for df in [pre_df, live_df]:
        df["att_full_buy"] = (df["att_money"] >= FULL_BUY_THRESHOLD).astype(int)
        df["def_full_buy"] = (df["def_money"] >= FULL_BUY_THRESHOLD).astype(int)
        df["att_eco"]      = (df["att_money"] < ECO_THRESHOLD).astype(int)
        df["def_eco"]      = (df["def_money"] < ECO_THRESHOLD).astype(int)
        df["ult_adv"]      = df["att_ults_ready"] - df["def_ults_ready"]

    # -- Live-round only features --
    total_alive = live_df["att_alive"] + live_df["def_alive"]
    live_df["alive_ratio"]   = live_df["att_alive"] / total_alive.replace(0, 1)  # avoid div/0
    live_df["kill_progress"] = (live_df["att_kills"] + live_df["def_kills"]) / 10.0  # max 10 kills in 5v5

    new_common = ["att_full_buy", "def_full_buy", "att_eco", "def_eco", "ult_adv"]
    new_live   = ["alive_ratio", "kill_progress"]

    print(f"\nSTEP 7: Feature Engineering")
    print(f"  Common features added: {new_common}")
    print(f"  Live-round features added: {new_live}")
    print(f"  Pre-round  total columns: {len(pre_df.columns)}")
    print(f"  Live-round total columns: {len(live_df.columns)}")

    return pre_df, live_df


# ------------------------------------------------------------------------------
# Step 8: Drop ID Columns Only (Keep All Raw + Diff Features)
# ------------------------------------------------------------------------------

def drop_columns(pre_df, live_df):
    """Drop only identifier columns. Keep ALL feature columns including
    both raw values and diffs -- the raw values carry signal beyond the diff.
    
    Example: economy_diff=0 at 5k vs 5k is different from 25k vs 25k.
    """
    # We need match_id for train/test split, so save it before dropping
    pre_match_ids  = pre_df["match_id"].copy()
    live_match_ids = live_df["match_id"].copy()

    # Drop ID columns only
    pre_df  = pre_df.drop(columns=[c for c in ID_COLUMNS if c in pre_df.columns])
    pre_df  = pre_df.drop(columns=["match_id"])
    live_df = live_df.drop(columns=[c for c in ID_COLUMNS if c in live_df.columns])
    live_df = live_df.drop(columns=["match_id"])

    pre_features  = [c for c in pre_df.columns if c != TARGET]
    live_features = [c for c in live_df.columns if c != TARGET]

    print(f"\nSTEP 8: Drop ID Columns (Keep All Feature Columns)")
    print(f"  Dropped: {ID_COLUMNS + ['match_id']}")
    print(f"  Pre-round  features ({len(pre_features)}): {pre_features}")
    print(f"  Live-round features ({len(live_features)}): {live_features}")

    return pre_df, live_df, pre_match_ids, live_match_ids


# ------------------------------------------------------------------------------
# Step 9: Train/Test Split (Grouped by Match ID)
# ------------------------------------------------------------------------------

def train_test_split_grouped(df, match_ids, dataset_name):
    """
    Split data into train/test sets, ensuring all rounds from the same
    match stay in the same split (prevents data leakage).
    """
    splitter = GroupShuffleSplit(n_splits=1, test_size=TEST_SIZE, random_state=RANDOM_STATE)

    train_idx, test_idx = next(splitter.split(df, df[TARGET], groups=match_ids))

    train_df = df.iloc[train_idx].reset_index(drop=True)
    test_df  = df.iloc[test_idx].reset_index(drop=True)

    # Verify no match_id overlap
    train_matches = set(match_ids.iloc[train_idx])
    test_matches  = set(match_ids.iloc[test_idx])
    overlap = train_matches & test_matches

    print(f"\n  {dataset_name}:")
    print(f"    Train: {len(train_df):,} rows ({len(train_matches)} matches)")
    print(f"    Test:  {len(test_df):,} rows ({len(test_matches)} matches)")
    print(f"    Match overlap: {len(overlap)} (should be 0)")
    print(f"    Train class balance: {train_df[TARGET].value_counts().to_dict()}")
    print(f"    Test  class balance: {test_df[TARGET].value_counts().to_dict()}")

    return train_df, test_df


# ------------------------------------------------------------------------------
# Step 10: Save Outputs
# ------------------------------------------------------------------------------

def save_outputs(pre_train, pre_test, live_train, live_test):
    """Save all processed DataFrames to CSV."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pre_train.to_csv(OUTPUT_DIR / "pre_round_train.csv",   index=False)
    pre_test.to_csv(OUTPUT_DIR / "pre_round_test.csv",     index=False)
    live_train.to_csv(OUTPUT_DIR / "live_round_train.csv", index=False)
    live_test.to_csv(OUTPUT_DIR / "live_round_test.csv",   index=False)

    print(f"\nSTEP 10: Save Outputs to {OUTPUT_DIR}/")
    print(f"  pre_round_train.csv   ({len(pre_train):,} rows)")
    print(f"  pre_round_test.csv    ({len(pre_test):,} rows)")
    print(f"  live_round_train.csv  ({len(live_train):,} rows)")
    print(f"  live_round_test.csv   ({len(live_test):,} rows)")


# ------------------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------------------

def validate(pre_train, pre_test, live_train, live_test):
    """Run sanity checks on the processed data."""
    print(f"\n{'=' * 70}")
    print("VALIDATION")
    print("=" * 70)

    all_dfs = {
        "pre_train":  pre_train,
        "pre_test":   pre_test,
        "live_train": live_train,
        "live_test":  live_test,
    }

    all_ok = True
    for name, df in all_dfs.items():
        nan_count = df.isnull().sum().sum()
        if nan_count > 0:
            print(f"  [FAIL] {name} has {nan_count} NaN values!")
            all_ok = False
        else:
            print(f"  [OK] {name}: {df.shape[0]:,} rows x {df.shape[1]} cols, no NaN values")

    if all_ok:
        print(f"\n  [OK] All validation checks passed!")
    else:
        print(f"\n  [FAIL] Some validation checks failed!")


# ------------------------------------------------------------------------------
# Main Pipeline
# ------------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("   VALORANT ROUND PREDICTION - DATA PREPROCESSING PIPELINE")
    print("=" * 70)

    # Step 1: Load
    pre_df, live_df = load_data()

    # Step 2: Drop NaN winners
    pre_df, live_df = drop_nan_winners(pre_df, live_df)

    # Step 3: Remove rare maps
    pre_df, live_df = remove_rare_maps(pre_df, live_df)

    # Step 4: Remove outliers (live-round only)
    live_df = remove_outliers(live_df)

    # Step 5: Encode target
    pre_df, live_df = encode_target(pre_df, live_df)

    # Step 6: Encode categoricals (label encode, NO scaling)
    pre_df, live_df = encode_categoricals(pre_df, live_df)

    # Step 7: Feature engineering
    pre_df, live_df = engineer_features(pre_df, live_df)

    # Step 8: Drop ID columns only (keep all raw + diff features)
    pre_df, live_df, pre_match_ids, live_match_ids = drop_columns(pre_df, live_df)

    # Step 9: Train/Test split (grouped by match_id)
    print(f"\nSTEP 9: Train/Test Split (Grouped by Match ID)")
    pre_train, pre_test   = train_test_split_grouped(pre_df, pre_match_ids, "Pre-Round")
    live_train, live_test = train_test_split_grouped(live_df, live_match_ids, "Live-Round")

    # Step 10: Save
    save_outputs(pre_train, pre_test, live_train, live_test)

    # Validate
    validate(pre_train, pre_test, live_train, live_test)

    print(f"\n{'=' * 70}")
    print("PREPROCESSING COMPLETE!")
    print("=" * 70)

    # Print summary
    pre_features  = [c for c in pre_train.columns if c != TARGET]
    live_features = [c for c in live_train.columns if c != TARGET]
    print(f"\nPre-Round (Model A):  {len(pre_features)} features, {len(pre_train):,} train / {len(pre_test):,} test")
    print(f"Live-Round (Model B): {len(live_features)} features, {len(live_train):,} train / {len(live_test):,} test")
    print(f"\nNotes: No scaling applied (XGBoost). Raw + diff features kept. Map/side label-encoded.")

    return pre_train, pre_test, live_train, live_test


if __name__ == "__main__":
    main()

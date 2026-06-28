"""
watcher.py
-----------
Watches raw_matchs_data/ for new JSON files.
When a new match JSON appears, automatically runs feature extraction
and appends the new rows to pre_round.csv and live_round.csv.

Usage:
    python watcher.py                                       # watches raw_matchs_data/
    python watcher.py --raw C:/overwolf/logs                # custom watch folder
    python watcher.py --raw C:/overwolf/logs --output out/  # custom output

Run this before you start playing. It stays running in the background.
Every time you finish a game and Overwolf saves a new .json, it gets
processed automatically and appended to your dataset.
"""

import time
import sys
import csv
import argparse
from pathlib import Path

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

# Resolve raw_matchs_data relative to this script's location
_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_RAW = str(_SCRIPT_DIR / ".." / "raw_matchs_data")
_DEFAULT_OUT = _DEFAULT_RAW  # CSVs go into the same raw data folder

# Import our extractor
sys.path.insert(0, str(_SCRIPT_DIR))
from extract_features import extract_from_file


class MatchHandler(FileSystemEventHandler if HAS_WATCHDOG else object):
    """Handles new JSON files by extracting features and appending to CSVs."""

    def __init__(self, output_dir, processed):
        self.output_dir = Path(output_dir)
        self.processed  = processed  # set of already-processed filenames
        self.pre_path   = self.output_dir / "pre_round.csv"
        self.live_path  = self.output_dir / "live_round.csv"

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix == ".json" and not path.name.startswith("."):
            time.sleep(2)  # wait for Overwolf to finish writing
            self._process(path)

    def _process(self, path):
        if path.name in self.processed:
            return
        self.processed.add(path.name)
        print(f"\n[NEW] {path.name}")
        try:
            pre, live = extract_from_file(path)
            if not pre:
                print("  No rounds extracted, skipping.")
                self.processed.discard(path.name)
                return

            labeled = sum(1 for r in pre if r["winner"])
            print(f"  Rounds: {len(pre)}  Kill events: {len(live)}  Labeled: {labeled}")

            self._append(pre,  self.pre_path)
            self._append(live, self.live_path)

            print(f"  Appended to {self.pre_path.name} + {self.live_path.name}")
            self._print_stats()

        except Exception as e:
            print(f"  ERROR: {e}")
            self.processed.discard(path.name)

    def _append(self, rows, path):
        """Append rows to CSV; write header only when creating a new file."""
        if not rows:
            return
        header = not path.exists()
        with open(path, "a" if path.exists() else "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if header:
                writer.writeheader()
            writer.writerows(rows)

    def _print_stats(self):
        for label, path in [("pre_round", self.pre_path), ("live_round", self.live_path)]:
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    n = sum(1 for _ in f) - 1
                print(f"  [{label}.csv total rows: {n}]")


def get_already_processed(output_dir):
    """Find JSON files already in pre_round.csv to avoid reprocessing."""
    pre_path = Path(output_dir) / "pre_round.csv"
    processed = set()
    if pre_path.exists():
        with open(pre_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed.add(row.get("source_file", ""))
    return processed


def fallback_poll(raw_dir, output_dir, interval=10):
    """Polling fallback when watchdog isn't installed."""
    raw_dir    = Path(raw_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    processed = get_already_processed(output_dir)
    handler   = MatchHandler(output_dir, processed)

    print(f"[Polling mode] Checking {raw_dir} every {interval}s for new .json files")
    print("Install watchdog for instant detection: pip install watchdog")
    print("Press Ctrl+C to stop.\n")

    while True:
        for path in sorted(raw_dir.glob("*.json")):
            if not path.name.startswith(".") and path.name not in processed:
                handler._process(path)
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(
        description="Watch for new match JSON files and auto-extract features."
    )
    parser.add_argument("--raw",    default=_DEFAULT_RAW, help="Folder to watch for new JSON files")
    parser.add_argument("--output", default=_DEFAULT_OUT, help="Output folder for CSVs")
    parser.add_argument("--poll-interval", type=int, default=10, help="Polling interval in seconds (fallback mode)")
    args = parser.parse_args()

    raw_dir    = Path(args.raw)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        print(f"Watch folder not found: {raw_dir}")
        print("Create it or pass --raw path/to/your/overwolf/output/")
        return

    processed = get_already_processed(output_dir)
    print(f"Already processed: {len(processed)} files")

    if not HAS_WATCHDOG:
        fallback_poll(raw_dir, output_dir, args.poll_interval)
        return

    handler  = MatchHandler(output_dir, processed)
    observer = Observer()
    observer.schedule(handler, str(raw_dir), recursive=False)
    observer.start()

    print(f"[Watching] {raw_dir}")
    print("Drop .json files here after each game. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    print("\nWatcher stopped.")


if __name__ == "__main__":
    main()
"""
Run specific state scrapers and report if new data was found.

Usage:
    python scripts/run_states.py NY NJ PA
    python scripts/run_states.py --backfill NY NJ PA

Sets GITHUB_OUTPUT new_data=true if any state got new rows.
"""

import importlib
import os
import signal
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from scrapers.config import STATE_REGISTRY

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
PER_STATE_TIMEOUT = 180  # 3 minutes per state


class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Scraper timed out")


def get_csv_fingerprint(state_code):
    """Get latest period_end and row count from state CSV."""
    csv_path = PROCESSED_DIR / f"{state_code}.csv"
    if not csv_path.exists():
        return None, 0
    try:
        df = pd.read_csv(csv_path, usecols=['period_end'], low_memory=False)
        return df['period_end'].max() if not df.empty else None, len(df)
    except Exception:
        return None, 0


def run_state(state_code, backfill=False):
    """Run a single state scraper. Returns (latest_before, latest_after, elapsed, error, changed)."""
    module_name = f"scrapers.{state_code.lower()}_scraper"
    class_name = f"{state_code}Scraper"

    latest_before, rows_before = get_csv_fingerprint(state_code)
    start = time.time()

    try:
        # Set timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(PER_STATE_TIMEOUT)

        mod = importlib.import_module(module_name)
        scraper_class = getattr(mod, class_name)
        scraper = scraper_class()
        df = scraper.run(backfill=backfill)

        signal.alarm(0)  # Cancel timeout

        latest_after, rows_after = get_csv_fingerprint(state_code)
        elapsed = time.time() - start
        changed = (latest_after != latest_before) or (rows_after != rows_before)

        return (latest_before, latest_after, elapsed, None, changed)

    except TimeoutError:
        signal.alarm(0)
        elapsed = time.time() - start
        return (rows_before, get_row_count(state_code), elapsed, "TIMEOUT", 0)

    except Exception as e:
        signal.alarm(0)
        elapsed = time.time() - start
        return (rows_before, get_row_count(state_code), elapsed, str(e)[:200], 0)


def main():
    args = sys.argv[1:]
    backfill = '--backfill' in args
    states = [s.upper() for s in args if not s.startswith('-')]

    if not states:
        print("No states specified. Usage: python run_states.py NY NJ PA")
        sys.exit(0)

    print(f"Running {len(states)} state(s): {' '.join(states)}")
    if backfill:
        print("  Mode: backfill")
    print()

    any_changed = False
    failures = []

    for sc in states:
        if sc not in STATE_REGISTRY:
            print(f"  {sc}: unknown state, skipping")
            continue

        name = STATE_REGISTRY[sc].get('name', sc)
        print(f"  {sc} ({name})...", end=' ', flush=True)

        latest_before, latest_after, elapsed, error, changed = run_state(sc, backfill)

        if error:
            print(f"FAIL ({elapsed:.0f}s): {error}")
            failures.append(sc)
        elif changed:
            print(f"NEW DATA: {latest_before} -> {latest_after} ({elapsed:.0f}s)")
            any_changed = True
        else:
            print(f"OK no new data ({elapsed:.0f}s)")

    # Summary
    print()
    if any_changed:
        print("NEW DATA DETECTED - dashboard update needed")
    else:
        print("No new data found.")

    if failures:
        print(f"FAILURES: {', '.join(failures)}")

    # Set GitHub Actions output
    _set_output('new_data', 'true' if any_changed else 'false')


def _set_output(name, value):
    gh_output = os.environ.get('GITHUB_OUTPUT')
    if gh_output:
        with open(gh_output, 'a') as f:
            f.write(f"{name}={value}\n")


if __name__ == '__main__':
    main()

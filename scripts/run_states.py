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


def get_row_count(state_code):
    """Count rows in state CSV."""
    csv_path = PROCESSED_DIR / f"{state_code}.csv"
    if not csv_path.exists():
        return 0
    try:
        return sum(1 for _ in open(csv_path)) - 1  # exclude header
    except Exception:
        return 0


def run_state(state_code, backfill=False):
    """Run a single state scraper. Returns (rows_before, rows_after, elapsed, error)."""
    module_name = f"scrapers.{state_code.lower()}_scraper"
    class_name = f"{state_code}Scraper"

    rows_before = get_row_count(state_code)
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

        rows_after = get_row_count(state_code)
        elapsed = time.time() - start
        new_rows = rows_after - rows_before

        return (rows_before, rows_after, elapsed, None, new_rows)

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

    total_new = 0
    failures = []

    for sc in states:
        if sc not in STATE_REGISTRY:
            print(f"  {sc}: unknown state, skipping")
            continue

        name = STATE_REGISTRY[sc].get('name', sc)
        print(f"  {sc} ({name})...", end=' ', flush=True)

        before, after, elapsed, error, new_rows = run_state(sc, backfill)

        if error:
            print(f"FAIL ({elapsed:.0f}s): {error}")
            failures.append(sc)
        elif new_rows > 0:
            print(f"OK +{new_rows} rows ({elapsed:.0f}s)")
            total_new += new_rows
        else:
            print(f"OK no new data ({elapsed:.0f}s)")

    # Summary
    print()
    if total_new > 0:
        print(f"NEW DATA: {total_new} new rows across {len(states)} states")
    else:
        print("No new data found.")

    if failures:
        print(f"FAILURES: {', '.join(failures)}")

    # Set GitHub Actions output
    _set_output('new_data', 'true' if total_new > 0 else 'false')
    _set_output('new_rows', str(total_new))


def _set_output(name, value):
    gh_output = os.environ.get('GITHUB_OUTPUT')
    if gh_output:
        with open(gh_output, 'a') as f:
            f.write(f"{name}={value}\n")


if __name__ == '__main__':
    main()

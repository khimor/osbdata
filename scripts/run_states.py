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
    """Get latest period_end and content hash of state CSV."""
    import hashlib
    csv_path = PROCESSED_DIR / f"{state_code}.csv"
    if not csv_path.exists():
        return None, None
    try:
        content = csv_path.read_bytes()
        content_hash = hashlib.md5(content).hexdigest()
        df = pd.read_csv(csv_path, usecols=['period_end'], low_memory=False)
        latest = df['period_end'].max() if not df.empty else None
        return latest, content_hash
    except Exception:
        return None, None


def run_state(state_code, backfill=False):
    """Run a single state scraper. Returns (latest_before, latest_after, elapsed, error, changed)."""
    module_name = f"scrapers.{state_code.lower()}_scraper"
    class_name = f"{state_code}Scraper"

    latest_before, hash_before = get_csv_fingerprint(state_code)
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

        latest_after, hash_after = get_csv_fingerprint(state_code)
        elapsed = time.time() - start
        # Only flag as changed if the CSV content actually changed
        # (hash comparison catches all cases: new periods, revised data, etc.)
        changed = hash_after is not None and hash_after != hash_before

        return (latest_before, latest_after, elapsed, None, changed)

    except TimeoutError:
        signal.alarm(0)
        elapsed = time.time() - start
        return (latest_before, None, elapsed, "TIMEOUT", 0)

    except Exception as e:
        signal.alarm(0)
        elapsed = time.time() - start
        return (latest_before, None, elapsed, str(e)[:200], 0)


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

    changed_states = []
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
            changed_states.append(sc)
        else:
            print(f"OK no new data ({elapsed:.0f}s)")

    # Summary
    print()
    if changed_states:
        print(f"NEW DATA DETECTED: {' '.join(changed_states)}")
    else:
        print("No new data found.")

    if failures:
        print(f"FAILURES: {', '.join(failures)}")

    # Set GitHub Actions outputs
    _set_output('new_data', 'true' if changed_states else 'false')
    _set_output('changed_states', ' '.join(changed_states))


def _set_output(name, value):
    gh_output = os.environ.get('GITHUB_OUTPUT')
    if gh_output:
        with open(gh_output, 'a') as f:
            f.write(f"{name}={value}\n")


if __name__ == '__main__':
    main()

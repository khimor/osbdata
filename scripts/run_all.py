#!/usr/bin/env python3
"""
Run scrapers for all states in priority order.
Usage: python scripts/run_all.py [--backfill] [--tier 1]
"""

import argparse
import importlib
import sys
import traceback
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.config import STATE_REGISTRY, get_states_by_tier


TIER_ORDER = [1, 2, 3, 4, 5]


def run_state(state_code: str, backfill: bool = False) -> dict:
    """Run a single state scraper. Returns result dict."""
    result = {
        'state': state_code,
        'status': 'pending',
        'rows': 0,
        'error': None,
        'duration_seconds': 0,
    }
    start = datetime.now()

    try:
        module = importlib.import_module(f"scrapers.{state_code.lower()}_scraper")
        scraper_class = None
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and name.endswith('Scraper') and name != 'BaseStateScraper':
                scraper_class = obj
                break

        if scraper_class is None:
            result['status'] = 'no_scraper'
            return result

        scraper = scraper_class()
        df = scraper.run(backfill=backfill)
        result['status'] = 'success'
        result['rows'] = len(df)
    except ModuleNotFoundError:
        result['status'] = 'no_scraper'
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        traceback.print_exc()

    result['duration_seconds'] = (datetime.now() - start).total_seconds()
    return result


def _copy_screenshots():
    """Copy source screenshots from data/raw/*/screenshots/ to dashboard/public/sources/."""
    import shutil
    raw_dir = Path("data/raw")
    dst_dir = Path("dashboard/public/sources")
    if not raw_dir.exists():
        return

    count = 0
    for state_dir in sorted(raw_dir.iterdir()):
        if not state_dir.is_dir():
            continue
        ss_dir = state_dir / "screenshots"
        if not ss_dir.exists():
            continue
        dst = dst_dir / state_dir.name / "screenshots"
        dst.mkdir(parents=True, exist_ok=True)
        for f in ss_dir.glob("*.png"):
            shutil.copy2(f, dst / f.name)
            count += 1

    if count:
        print(f"\nCopied {count} screenshot(s) to {dst_dir}")


def main():
    parser = argparse.ArgumentParser(description="Run all state scrapers")
    parser.add_argument("--backfill", action="store_true", help="Download all history")
    parser.add_argument("--tier", type=int, help="Only run states in this tier")
    args = parser.parse_args()

    tiers = [args.tier] if args.tier else TIER_ORDER
    results = []

    for tier in tiers:
        states = get_states_by_tier(tier)
        print(f"\n{'='*60}")
        print(f"TIER {tier}: {', '.join(states)}")
        print(f"{'='*60}")

        for state in states:
            print(f"\n--- {state} ({STATE_REGISTRY[state]['name']}) ---")
            result = run_state(state, backfill=args.backfill)
            results.append(result)

            status_icon = {'success': '✓', 'error': '✗', 'no_scraper': '⊘', 'pending': '?'}
            icon = status_icon.get(result['status'], '?')
            print(f"  {icon} {state}: {result['status']} ({result['rows']} rows, {result['duration_seconds']:.1f}s)")
            if result['error']:
                print(f"    Error: {result['error'][:200]}")

    # Copy screenshots to dashboard public directory for serving
    _copy_screenshots()

    # Sync CSVs to dashboard
    import shutil
    processed = Path("data/processed")
    public_data = Path("dashboard/public/data")
    if processed.exists() and public_data.exists():
        csv_count = 0
        for csv in processed.glob("??.csv"):
            shutil.copy2(csv, public_data / csv.name)
            csv_count += 1
        if csv_count:
            print(f"Synced {csv_count} CSV(s) to {public_data}")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for r in results:
        icon = {'success': '✓', 'error': '✗', 'no_scraper': '⊘'}.get(r['status'], '?')
        print(f"  {icon} {r['state']}: {r['status']} ({r['rows']} rows)")

    success = sum(1 for r in results if r['status'] == 'success')
    errors = sum(1 for r in results if r['status'] == 'error')
    missing = sum(1 for r in results if r['status'] == 'no_scraper')
    total_rows = sum(r['rows'] for r in results)
    print(f"\n  Success: {success}, Errors: {errors}, No scraper: {missing}")
    print(f"  Total rows: {total_rows}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Run scraper for a single state.
Usage: python scripts/run_state.py --state NY [--backfill]
"""

import argparse
import importlib
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Run a state sports betting scraper")
    parser.add_argument("--state", required=True, help="State code (e.g., NY, PA)")
    parser.add_argument("--backfill", action="store_true", help="Download all history (not just new)")
    args = parser.parse_args()

    state_code = args.state.upper()

    try:
        module = importlib.import_module(f"scrapers.{state_code.lower()}_scraper")
    except ModuleNotFoundError:
        print(f"Error: No scraper found for state '{state_code}'")
        print(f"Expected file: scrapers/{state_code.lower()}_scraper.py")
        sys.exit(1)

    # Find the scraper class (convention: {StateCode}Scraper)
    scraper_class = None
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and name.endswith('Scraper') and name != 'BaseStateScraper':
            scraper_class = obj
            break

    if scraper_class is None:
        print(f"Error: No scraper class found in scrapers/{state_code.lower()}_scraper.py")
        sys.exit(1)

    print(f"Running {state_code} scraper (backfill={args.backfill})...")
    scraper = scraper_class()
    df = scraper.run(backfill=args.backfill)
    print(f"Done: {len(df)} rows")


if __name__ == "__main__":
    main()

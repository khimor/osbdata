"""
One-time bulk load of all state CSV data into Supabase.
Reads data/processed/*.csv and upserts into the monthly_data table.

Usage:
    python scripts/load_to_supabase.py              # load all states
    python scripts/load_to_supabase.py NY NJ PA     # load specific states
"""

import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.db import upsert_dataframe
from scrapers.config import STATE_REGISTRY

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def load_state(state_code):
    """Load a single state CSV into Supabase."""
    csv_path = PROCESSED_DIR / f"{state_code}.csv"
    if not csv_path.exists():
        print(f"  {state_code}: CSV not found, skipping")
        return 0

    df = pd.read_csv(csv_path, low_memory=False)
    if df.empty:
        print(f"  {state_code}: empty CSV, skipping")
        return 0

    # Clean up columns for DB compatibility
    # Replace NaN with None for proper null handling
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].where(df[col].notna(), None)
            df[col] = df[col].replace({'': None, 'nan': None, 'None': None, 'NA': None})

    # Ensure sport_category nulls are consistent (empty string → None)
    if 'sport_category' in df.columns:
        df['sport_category'] = df['sport_category'].where(
            df['sport_category'].notna() & (df['sport_category'] != ''), None
        )

    start = time.time()
    result = upsert_dataframe(df)
    elapsed = time.time() - start

    inserted = result.get('inserted', 0)
    errors = result.get('errors', 0)

    if errors:
        print(f"  {state_code}: {inserted} rows, {errors} errors ({elapsed:.1f}s)")
    else:
        print(f"  {state_code}: {inserted} rows ({elapsed:.1f}s)")

    return inserted


def main():
    args = [s.upper() for s in sys.argv[1:] if not s.startswith('-')]
    states = args if args else sorted(STATE_REGISTRY.keys())

    print(f"Loading {len(states)} states into Supabase")
    print("=" * 50)

    total_rows = 0
    failures = []

    for i, sc in enumerate(states, 1):
        name = STATE_REGISTRY.get(sc, {}).get('name', sc)
        print(f"[{i}/{len(states)}] {sc} ({name})...", end=' ', flush=True)

        try:
            rows = load_state(sc)
            total_rows += rows
        except Exception as e:
            print(f"  {sc}: FAILED - {e}")
            failures.append(sc)

    print(f"\n{'=' * 50}")
    print(f"Total: {total_rows:,} rows loaded")
    if failures:
        print(f"Failures: {', '.join(failures)}")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Data quality validation across all scraped state data.
Usage: python scripts/validate.py [--state NY]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.config import STATE_REGISTRY


def load_data(state_code: str = None) -> pd.DataFrame:
    """Load processed data."""
    processed = Path("data/processed")
    if state_code:
        path = processed / f"{state_code}.csv"
        if not path.exists():
            print(f"No data for {state_code}")
            return pd.DataFrame()
        return pd.read_csv(path)

    # Load all
    all_dfs = []
    for csv_path in sorted(processed.glob("*.csv")):
        if csv_path.name == "all_states.csv":
            continue
        try:
            all_dfs.append(pd.read_csv(csv_path))
        except Exception as e:
            print(f"Error loading {csv_path}: {e}")
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


def check_duplicates(df: pd.DataFrame):
    """Check for duplicate rows."""
    key_cols = ['state_code', 'period_end', 'period_type', 'operator_standard', 'channel', 'sport_category']
    existing = [c for c in key_cols if c in df.columns]
    dupes = df[df.duplicated(subset=existing, keep=False)]
    if len(dupes) > 0:
        print(f"  FAIL: {len(dupes)} duplicate rows")
        for _, row in dupes.head(5).iterrows():
            print(f"    {row['state_code']} {row['period_end']} {row.get('operator_standard', '')} {row.get('channel', '')}")
    else:
        print(f"  OK: No duplicates")
    return len(dupes)


def check_negative_handle(df: pd.DataFrame):
    """Check for negative handle values."""
    if 'handle' not in df.columns:
        return 0
    neg = df[df['handle'].notna() & (df['handle'] < 0)]
    if len(neg) > 0:
        print(f"  WARN: {len(neg)} rows with negative handle")
        for _, row in neg.head(3).iterrows():
            print(f"    {row['state_code']} {row['period_end']}: handle={row['handle']}")
    else:
        print(f"  OK: No negative handles")
    return len(neg)


def check_hold_sanity(df: pd.DataFrame):
    """Check that hold % is in reasonable range."""
    if 'hold_pct' not in df.columns:
        return 0
    insane = df[df['hold_pct'].notna() & ((df['hold_pct'] < 0.01) | (df['hold_pct'] > 0.40))]
    if len(insane) > 0:
        print(f"  WARN: {len(insane)} rows with unusual hold% (outside 1%-40%)")
        for _, row in insane.head(5).iterrows():
            print(f"    {row['state_code']} {row['period_end']} {row.get('operator_standard', '')}: hold={row['hold_pct']:.4f}")
    else:
        print(f"  OK: All hold% values in range")
    return len(insane)


def check_ggr_vs_handle(df: pd.DataFrame):
    """Check that GGR <= Handle."""
    if 'gross_revenue' not in df.columns or 'handle' not in df.columns:
        return 0
    bad = df[df['gross_revenue'].notna() & df['handle'].notna() & (df['handle'] > 0) & (df['gross_revenue'] > df['handle'])]
    if len(bad) > 0:
        print(f"  FAIL: {len(bad)} rows where GGR > Handle")
    else:
        print(f"  OK: GGR <= Handle for all rows")
    return len(bad)


def check_unresolved_operators(df: pd.DataFrame):
    """Check for operator names that weren't normalized."""
    if 'operator_raw' not in df.columns or 'operator_standard' not in df.columns:
        return 0
    unresolved = df[
        (df['operator_standard'] == df['operator_raw']) &
        (~df['operator_raw'].isin(['ALL', 'TOTAL', 'UNKNOWN', None, '']))
    ]
    if len(unresolved) > 0:
        unique_names = unresolved['operator_raw'].unique()
        print(f"  WARN: {len(unique_names)} unresolved operator names:")
        for name in sorted(unique_names)[:10]:
            states = unresolved[unresolved['operator_raw'] == name]['state_code'].unique()
            print(f"    '{name}' (in {', '.join(states)})")
    else:
        print(f"  OK: All operator names resolved")
    return len(unresolved['operator_raw'].unique()) if len(unresolved) > 0 else 0


def check_handle_magnitude(df: pd.DataFrame):
    """Check handle magnitudes are reasonable."""
    issues = 0
    monthly = df[df['period_type'] == 'monthly']
    if monthly.empty:
        return 0

    for state in monthly['state_code'].unique():
        state_data = monthly[monthly['state_code'] == state]
        tier = STATE_REGISTRY.get(state, {}).get('tier', 99)

        # Sum handle by period (across operators)
        totals = state_data.groupby('period_end')['handle'].sum()
        if totals.empty:
            continue

        avg_handle = totals.mean()
        if pd.isna(avg_handle):
            continue

        # Tier 1-2: expect $100M+ monthly handle (= 10B cents)
        if tier <= 2 and avg_handle < 1_000_000_000:  # $10M in cents
            print(f"  WARN: {state} avg monthly handle ${avg_handle/100:,.0f} — low for Tier {tier}")
            issues += 1
        # Any state: < $10K monthly is almost certainly wrong
        if avg_handle < 1_000_000 and avg_handle > 0:  # $10K in cents
            print(f"  FAIL: {state} avg monthly handle ${avg_handle/100:,.0f} — likely parsing error")
            issues += 1

    if issues == 0:
        print(f"  OK: Handle magnitudes look reasonable")
    return issues


def check_date_gaps(df: pd.DataFrame):
    """Check for gaps in monthly reporting."""
    issues = 0
    monthly = df[df['period_type'] == 'monthly']
    if monthly.empty:
        return 0

    for state in monthly['state_code'].unique():
        state_data = monthly[monthly['state_code'] == state]
        dates = pd.to_datetime(state_data['period_end']).dt.to_period('M').unique()
        dates = sorted(dates)

        if len(dates) < 2:
            continue

        gaps = []
        for i in range(1, len(dates)):
            diff = (dates[i] - dates[i-1]).n
            if diff > 1:
                gaps.append((dates[i-1], dates[i], diff))

        if gaps:
            print(f"  WARN: {state} has {len(gaps)} gaps in monthly data:")
            for start, end, months in gaps[:3]:
                print(f"    Gap: {start} → {end} ({months} months)")
            issues += len(gaps)

    if issues == 0:
        print(f"  OK: No date gaps detected")
    return issues


def main():
    parser = argparse.ArgumentParser(description="Validate scraped data quality")
    parser.add_argument("--state", help="Validate specific state only")
    args = parser.parse_args()

    print("=" * 60)
    print("DATA QUALITY VALIDATION")
    print("=" * 60)

    df = load_data(args.state.upper() if args.state else None)
    if df.empty:
        print("No data to validate")
        sys.exit(0)

    states = df['state_code'].unique()
    rows = len(df)
    print(f"Loaded {rows:,} rows across {len(states)} states: {', '.join(sorted(states))}\n")

    total_issues = 0

    print("1. Duplicate Check")
    total_issues += check_duplicates(df)

    print("\n2. Negative Handle Check")
    total_issues += check_negative_handle(df)

    print("\n3. Hold % Sanity Check")
    total_issues += check_hold_sanity(df)

    print("\n4. GGR vs Handle Check")
    total_issues += check_ggr_vs_handle(df)

    print("\n5. Unresolved Operators")
    total_issues += check_unresolved_operators(df)

    print("\n6. Handle Magnitude Check")
    total_issues += check_handle_magnitude(df)

    print("\n7. Date Gap Check")
    total_issues += check_date_gaps(df)

    print(f"\n{'='*60}")
    if total_issues == 0:
        print("ALL CHECKS PASSED")
    else:
        print(f"FOUND {total_issues} ISSUES — review above")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

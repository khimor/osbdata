"""
Data Quality Audit Script
Scans all state CSVs in data/processed/ and flags anomalies.
Output: data/audit_results.csv

Usage:
    python3 scripts/data_audit.py
"""

import sys
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent.parent / "data" / "processed"
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "audit_results.csv"

# Two-letter state codes only (skip supplementary files like NJ_handle.csv)
import re
STATE_CSV_PATTERN = re.compile(r'^[A-Z]{2}\.csv$')

# Thresholds (values in cents)
EXTREME_HANDLE_THRESHOLD = 5_000_000_000_000  # $50B
EXTREME_HOLD_THRESHOLD = 1.0   # 100%
SUSPICIOUS_HOLD_THRESHOLD = 0.50  # 50%


def load_all_csvs():
    """Load and concatenate all state CSVs."""
    frames = []
    for f in sorted(DATA_DIR.glob("*.csv")):
        if not STATE_CSV_PATTERN.match(f.name):
            continue
        try:
            df = pd.read_csv(f, low_memory=False)
            df['_source_file'] = f.name
            df['_row_idx'] = range(len(df))
            frames.append(df)
        except Exception as e:
            print(f"  SKIP {f.name}: {e}")
    if not frames:
        print("No CSV files found!")
        sys.exit(1)
    return pd.concat(frames, ignore_index=True)


def to_float(val):
    """Safely convert to float."""
    try:
        if pd.isna(val):
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def audit_row(row, idx):
    """Check a single row for anomalies. Returns list of issue dicts."""
    issues = []
    state = row.get('state_code', '')
    period = row.get('period_end', '')
    operator = row.get('operator_standard', '')
    channel = row.get('channel', '')

    base = {
        'state': state,
        'period_end': period,
        'operator': operator,
        'channel': channel,
        'row_index': idx,
    }

    handle = to_float(row.get('handle'))
    ggr = to_float(row.get('gross_revenue'))
    std_ggr = to_float(row.get('standard_ggr'))
    payouts = to_float(row.get('payouts'))
    hold = to_float(row.get('hold_pct'))
    tax = to_float(row.get('tax_paid'))

    # 1. Negative handle
    if handle is not None and handle < 0:
        sev = 'HIGH' if abs(handle) > 100_000_000 else 'MEDIUM'
        issues.append({
            **base,
            'issue_type': 'negative_handle',
            'severity': sev,
            'current_value': f"${handle/100:,.2f}",
            'expected_value': '>= $0',
            'notes': f"Handle is negative: ${handle/100:,.2f}",
        })

    # 2. Extreme hold (> 100% or < -100%)
    if hold is not None and (hold > EXTREME_HOLD_THRESHOLD or hold < -EXTREME_HOLD_THRESHOLD):
        issues.append({
            **base,
            'issue_type': 'extreme_hold',
            'severity': 'HIGH',
            'current_value': f"{hold*100:.2f}%",
            'expected_value': 'Between -100% and 100%',
            'notes': f"Hold of {hold*100:.2f}% is likely erroneous",
        })

    # 3. GGR exceeds handle
    ggr_val = std_ggr if std_ggr is not None else ggr
    if (handle is not None and handle > 0 and
            ggr_val is not None and ggr_val > handle):
        pct = (ggr_val / handle * 100) if handle else 0
        issues.append({
            **base,
            'issue_type': 'ggr_exceeds_handle',
            'severity': 'HIGH',
            'current_value': f"GGR ${ggr_val/100:,.2f} vs Handle ${handle/100:,.2f}",
            'expected_value': 'GGR <= Handle',
            'notes': f"GGR is {pct:.1f}% of handle (impossible without accounting adjustments)",
        })

    # 4. Extreme handle (> $50B per row)
    if handle is not None and handle > EXTREME_HANDLE_THRESHOLD:
        issues.append({
            **base,
            'issue_type': 'extreme_handle',
            'severity': 'HIGH',
            'current_value': f"${handle/100:,.2f}",
            'expected_value': f"< ${EXTREME_HANDLE_THRESHOLD/100:,.0f}",
            'notes': 'Likely unit error (cents stored as dollars or doubled)',
        })

    # 5. Zero handle with non-zero GGR
    if (handle is not None and handle == 0 and
            ggr_val is not None and ggr_val != 0):
        issues.append({
            **base,
            'issue_type': 'zero_handle_nonzero_ggr',
            'severity': 'MEDIUM',
            'current_value': f"Handle $0, GGR ${ggr_val/100:,.2f}",
            'expected_value': 'Handle > 0 when GGR != 0',
            'notes': 'Zero handle but non-zero GGR',
        })

    # 6. Suspicious hold (> 50% but < 100%)
    if (hold is not None and
            abs(hold) > SUSPICIOUS_HOLD_THRESHOLD and
            abs(hold) <= EXTREME_HOLD_THRESHOLD):
        issues.append({
            **base,
            'issue_type': 'suspicious_hold',
            'severity': 'LOW',
            'current_value': f"{hold*100:.2f}%",
            'expected_value': 'Typically 5-15%',
            'notes': 'May be legitimate for small retail venues',
        })

    return issues


def audit_state_level(df, state_code):
    """State-level checks: duplicates, date gaps."""
    issues = []
    state_df = df[df['state_code'] == state_code].copy()

    # Duplicates — use operator_reported (not operator_standard, which consolidates venues)
    dup_cols = ['state_code', 'period_end', 'operator_reported', 'channel', 'period_type']
    existing_cols = [c for c in dup_cols if c in state_df.columns]
    # Only check non-sport rows for duplicates
    non_sport = state_df[state_df['sport_category'].isna() | (state_df['sport_category'] == '')]
    dupes = non_sport[non_sport.duplicated(subset=existing_cols, keep=False)]
    if len(dupes) > 0:
        dup_groups = dupes.groupby(existing_cols).size()
        for key, count in dup_groups.items():
            if count > 1:
                issues.append({
                    'state': state_code,
                    'period_end': str(key[1]) if len(key) > 1 else '',
                    'operator': str(key[2]) if len(key) > 2 else '',
                    'channel': str(key[3]) if len(key) > 3 else '',
                    'issue_type': 'duplicate_rows',
                    'severity': 'HIGH',
                    'current_value': f"{count} duplicates",
                    'expected_value': '1 row',
                    'notes': f"Duplicate key: {key}",
                    'row_index': '',
                })

    # Date gaps in monthly data
    monthly = state_df[state_df['period_type'] == 'monthly']
    if len(monthly) > 0:
        periods = sorted(monthly['period_end'].dropna().unique())
        for i in range(1, len(periods)):
            try:
                prev = pd.Timestamp(periods[i - 1])
                curr = pd.Timestamp(periods[i])
                gap_months = (curr.year - prev.year) * 12 + (curr.month - prev.month)
                if gap_months > 2:
                    issues.append({
                        'state': state_code,
                        'period_end': str(periods[i]),
                        'operator': '',
                        'channel': '',
                        'issue_type': 'date_gap',
                        'severity': 'LOW',
                        'current_value': f"{gap_months} month gap",
                        'expected_value': '1 month gap',
                        'notes': f"Gap from {periods[i-1]} to {periods[i]}",
                        'row_index': '',
                    })
            except Exception:
                continue

    return issues


def main():
    print("=" * 60)
    print("DATA QUALITY AUDIT")
    print("=" * 60)

    print(f"\nLoading CSVs from {DATA_DIR}...")
    df = load_all_csvs()
    print(f"Loaded {len(df):,} rows across {df['state_code'].nunique()} states")

    all_issues = []

    # Row-level checks (skip sport_category rows)
    print("\nRunning row-level checks...")
    non_sport = df[df['sport_category'].isna() | (df['sport_category'] == '')]
    for idx, row in non_sport.iterrows():
        issues = audit_row(row, idx)
        all_issues.extend(issues)

    # State-level checks
    print("Running state-level checks...")
    for state in sorted(df['state_code'].unique()):
        issues = audit_state_level(df, state)
        all_issues.extend(issues)

    # Output
    if all_issues:
        result_df = pd.DataFrame(all_issues)
        col_order = ['state', 'period_end', 'operator', 'channel',
                      'issue_type', 'severity', 'current_value',
                      'expected_value', 'notes', 'row_index']
        existing = [c for c in col_order if c in result_df.columns]
        result_df = result_df[existing]
        result_df.to_csv(OUTPUT_PATH, index=False)
        print(f"\nWrote {len(result_df):,} issues to {OUTPUT_PATH}")

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)

        by_severity = result_df['severity'].value_counts()
        for sev in ['HIGH', 'MEDIUM', 'LOW']:
            if sev in by_severity.index:
                print(f"  {sev}: {by_severity[sev]:,}")

        print(f"\nBy issue type:")
        for issue_type, count in result_df['issue_type'].value_counts().items():
            print(f"  {issue_type}: {count:,}")

        print(f"\nTop states by HIGH severity issues:")
        high = result_df[result_df['severity'] == 'HIGH']
        if len(high) > 0:
            top = high['state'].value_counts().head(10)
            for state, count in top.items():
                print(f"  {state}: {count:,}")
    else:
        print("\nNo issues found!")

    print("\nDone.")


if __name__ == "__main__":
    main()

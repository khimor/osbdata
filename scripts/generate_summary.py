"""
Generate a plain-text summary of scraper results for email notifications.
Reads the run_states.py output and CSV changes to produce a concise update.

Usage:
    python scripts/generate_summary.py [state1 state2 ...]

Outputs summary to stdout and writes to /tmp/scrape_summary.txt
"""

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from scrapers.config import STATE_REGISTRY

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def format_dollars(cents):
    if cents is None or pd.isna(cents):
        return "-"
    dollars = abs(float(cents)) / 100
    if dollars >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.2f}B"
    if dollars >= 1_000_000:
        return f"${dollars / 1_000_000:.1f}M"
    if dollars >= 1_000:
        return f"${dollars / 1_000:.1f}K"
    return f"${dollars:.0f}"


def get_state_summary(state_code):
    csv_path = PROCESSED_DIR / f"{state_code}.csv"
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path, low_memory=False)
    monthly = df[(df['period_type'] == 'monthly') & (df['sport_category'].isna() | (df['sport_category'] == ''))]

    if monthly.empty:
        return None

    periods = sorted(monthly['period_end'].dropna().unique())
    latest_period = periods[-1]
    latest = monthly[monthly['period_end'] == latest_period]

    # Exclude TOTAL rows if operator rows exist
    ops = latest[~latest['operator_standard'].isin(['TOTAL', 'ALL'])]
    totals = latest[latest['operator_standard'].isin(['TOTAL', 'ALL'])]
    use = ops if len(ops) > 0 else latest

    handle = use['handle'].sum()
    if handle == 0 and len(totals) > 0:
        handle = totals['handle'].sum()

    ggr = use['standard_ggr'].fillna(use['gross_revenue']).fillna(0).sum()
    if ggr == 0 and len(totals) > 0:
        ggr = totals['standard_ggr'].fillna(totals['gross_revenue']).fillna(0).sum()

    hold = (ggr / handle) if handle > 0 else None
    n_ops = len(ops['operator_standard'].unique()) if len(ops) > 0 else 0

    return {
        'state': state_code,
        'name': STATE_REGISTRY.get(state_code, {}).get('name', state_code),
        'period': latest_period,
        'handle': handle,
        'ggr': ggr,
        'hold': hold,
        'operators': n_ops,
        'total_rows': len(df),
    }


def main():
    states = [s.upper() for s in sys.argv[1:] if not s.startswith('-')]

    # If no states specified, summarize all
    if not states:
        states = sorted(STATE_REGISTRY.keys())

    lines = []
    lines.append("OSB TRACKER - Data Update Summary")
    lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"{'State':<6} {'Period':<12} {'Handle':>14} {'GGR':>14} {'Hold':>8} {'Ops':>5}")
    lines.append("-" * 65)

    total_handle = 0
    total_ggr = 0
    updated_count = 0

    for sc in states:
        summary = get_state_summary(sc)
        if not summary:
            continue

        hold_str = f"{summary['hold']*100:.1f}%" if summary['hold'] else "-"
        lines.append(
            f"{summary['state']:<6} {summary['period']:<12} "
            f"{format_dollars(summary['handle']):>14} "
            f"{format_dollars(summary['ggr']):>14} "
            f"{hold_str:>8} "
            f"{summary['operators']:>5}"
        )
        total_handle += summary['handle'] or 0
        total_ggr += summary['ggr'] or 0
        updated_count += 1

    lines.append("-" * 65)
    lines.append(
        f"{'TOTAL':<6} {'':12} "
        f"{format_dollars(total_handle):>14} "
        f"{format_dollars(total_ggr):>14}"
    )
    lines.append(f"\n{updated_count} state(s) included in this update.")

    output = "\n".join(lines)
    print(output)

    # Write to file for email step
    with open("/tmp/scrape_summary.txt", "w") as f:
        f.write(output)


if __name__ == "__main__":
    main()

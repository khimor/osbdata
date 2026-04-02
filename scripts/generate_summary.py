"""
Generate a summary of scraper results for email notifications.
Outputs both plain text and structured JSON.

Usage:
    python scripts/generate_summary.py [state1 state2 ...]

Outputs:
    /tmp/scrape_summary.txt  - plain text summary
    /tmp/scrape_summary.json - structured JSON for email templating
"""

import json
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
    sign = "-" if float(cents) < 0 else ""
    if dollars >= 1_000_000_000:
        return f"{sign}${dollars / 1_000_000_000:.2f}B"
    if dollars >= 1_000_000:
        return f"{sign}${dollars / 1_000_000:.1f}M"
    if dollars >= 1_000:
        return f"{sign}${dollars / 1_000:.1f}K"
    return f"{sign}${dollars:.0f}"


def get_state_summary(state_code):
    csv_path = PROCESSED_DIR / f"{state_code}.csv"
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path, low_memory=False)
    monthly = df[(df['period_type'] == 'monthly') & (df['sport_category'].isna() | (df['sport_category'] == ''))]

    if monthly.empty:
        monthly = df[df['sport_category'].isna() | (df['sport_category'] == '')]
    if monthly.empty:
        return None

    periods = sorted(monthly['period_end'].dropna().unique())
    latest_period = periods[-1]
    latest = monthly[monthly['period_end'] == latest_period]

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

    # YoY comparison
    yoy_pct = None
    if latest_period and handle > 0:
        d = datetime.strptime(str(latest_period)[:10], '%Y-%m-%d')
        yoy_month = f"{d.year - 1}-{d.month:02d}"
        yoy_periods = [p for p in periods if p.startswith(yoy_month)]
        if yoy_periods:
            yoy_rows = monthly[monthly['period_end'] == yoy_periods[-1]]
            yoy_ops = yoy_rows[~yoy_rows['operator_standard'].isin(['TOTAL', 'ALL'])]
            yoy_totals = yoy_rows[yoy_rows['operator_standard'].isin(['TOTAL', 'ALL'])]
            yoy_use = yoy_ops if len(yoy_ops) > 0 else yoy_rows
            yoy_handle = yoy_use['handle'].sum()
            if yoy_handle == 0 and len(yoy_totals) > 0:
                yoy_handle = yoy_totals['handle'].sum()
            if yoy_handle > 0:
                yoy_pct = (handle - yoy_handle) / abs(yoy_handle)

    # Determine period type from latest rows
    period_type = 'monthly'
    if 'period_type' in latest.columns:
        pt = latest['period_type'].dropna().unique()
        if len(pt) > 0:
            period_type = pt[0]

    return {
        'state': state_code,
        'name': STATE_REGISTRY.get(state_code, {}).get('name', state_code),
        'period': latest_period,
        'period_type': period_type,
        'handle': handle,
        'ggr': ggr,
        'hold': hold,
        'operators': n_ops,
        'total_rows': len(df),
        'yoy_handle_pct': yoy_pct,
    }


def main():
    states = [s.upper() for s in sys.argv[1:] if not s.startswith('-')]
    if not states:
        states = sorted(STATE_REGISTRY.keys())

    # Build summaries
    summaries = {}
    for sc in states:
        s = get_state_summary(sc)
        if s:
            summaries[sc] = s

    # --- Plain text output ---
    lines = []
    lines.append("OSB TRACKER - Data Update Summary")
    lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"{'State':<6} {'Period':<12} {'Handle':>14} {'GGR':>14} {'Hold':>8} {'YoY':>8} {'Ops':>5}")
    lines.append("-" * 73)

    total_handle = 0
    total_ggr = 0

    for sc in sorted(summaries.keys()):
        s = summaries[sc]
        hold_str = f"{s['hold']*100:.1f}%" if s['hold'] else "-"
        yoy_str = f"{s['yoy_handle_pct']*100:+.1f}%" if s['yoy_handle_pct'] is not None else "-"
        lines.append(
            f"{s['state']:<6} {s['period']:<12} "
            f"{format_dollars(s['handle']):>14} "
            f"{format_dollars(s['ggr']):>14} "
            f"{hold_str:>8} "
            f"{yoy_str:>8} "
            f"{s['operators']:>5}"
        )
        total_handle += s['handle'] or 0
        total_ggr += s['ggr'] or 0

    lines.append("-" * 73)
    lines.append(
        f"{'TOTAL':<6} {'':12} "
        f"{format_dollars(total_handle):>14} "
        f"{format_dollars(total_ggr):>14}"
    )
    lines.append(f"\n{len(summaries)} state(s) included.")

    text_output = "\n".join(lines)
    print(text_output)
    with open("/tmp/scrape_summary.txt", "w") as f:
        f.write(text_output)

    # --- JSON output ---
    json_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "updated_states": sorted(summaries.keys()),
        "states": {},
        "total_handle": total_handle,
        "total_ggr": total_ggr,
    }

    for sc, s in summaries.items():
        json_data["states"][sc] = {
            "name": s['name'],
            "period": s['period'],
            "period_type": s.get('period_type', 'monthly'),
            "handle": s['handle'],
            "handle_formatted": format_dollars(s['handle']),
            "ggr": s['ggr'],
            "ggr_formatted": format_dollars(s['ggr']),
            "hold_pct": s['hold'],
            "operators": s['operators'],
            "yoy_handle_pct": s['yoy_handle_pct'],
        }

    with open("/tmp/scrape_summary.json", "w") as f:
        json.dump(json_data, f, indent=2, default=lambda x: int(x) if hasattr(x, 'item') else float(x))

    print(f"\nJSON written to /tmp/scrape_summary.json")


if __name__ == "__main__":
    main()

"""
QA Data Agent — validates all CSV data for sanity before it reaches clients.
Catches scraper errors, unit mismatches, parsing failures, and anomalies.

Usage:
    python tests/qa_data.py                    # check all states
    python tests/qa_data.py --states NY NJ PA  # check specific states
    python tests/qa_data.py --json             # output JSON report

Exit code: 0 = all pass, 1 = warnings only, 2 = critical failures
"""

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from scrapers.config import STATE_REGISTRY

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

# Thresholds (all values in cents)
HANDLE_MIN = 10_000_00          # $10K minimum per state/month
HANDLE_MAX = 500_000_000_00_00  # $5B maximum per state/month
GGR_MIN = 1_000_00              # $1K minimum
GGR_MAX = 50_000_000_000_00     # $500M maximum
HOLD_STATE_MIN = 0.01           # 1% hold for state total
HOLD_STATE_MAX = 0.25           # 25% hold for state total
HOLD_OP_MIN = -0.10             # -10% hold for individual operator
HOLD_OP_MAX = 0.50              # 50% hold for individual operator
MOM_SPIKE = 10.0                # 10x month-over-month = suspicious
MOM_DROP = 0.1                  # 0.1x month-over-month = suspicious
MAX_OPERATORS = 50
MAX_DAYS_STALE = 90             # State should have data within 90 days


class QACheck:
    def __init__(self, name, severity, state, message, value=None, expected=None):
        self.name = name
        self.severity = severity  # CRITICAL, WARNING, INFO
        self.state = state
        self.message = message
        self.value = value
        self.expected = expected

    def to_dict(self):
        return {
            'name': self.name,
            'severity': self.severity,
            'state': self.state,
            'message': self.message,
            'value': str(self.value) if self.value is not None else None,
            'expected': str(self.expected) if self.expected is not None else None,
        }


def fmt(cents):
    if cents is None or pd.isna(cents):
        return 'N/A'
    d = abs(float(cents)) / 100
    sign = '-' if float(cents) < 0 else ''
    if d >= 1e9: return f'{sign}${d/1e9:.1f}B'
    if d >= 1e6: return f'{sign}${d/1e6:.1f}M'
    if d >= 1e3: return f'{sign}${d/1e3:.0f}K'
    return f'{sign}${d:.0f}'


def check_state(state_code):
    """Run all sanity checks for a single state. Returns list of QACheck."""
    issues = []
    csv_path = PROCESSED_DIR / f"{state_code}.csv"

    if not csv_path.exists():
        issues.append(QACheck('missing_csv', 'CRITICAL', state_code, f'No CSV file found'))
        return issues

    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        issues.append(QACheck('csv_read_error', 'CRITICAL', state_code, f'Cannot read CSV: {e}'))
        return issues

    if df.empty:
        issues.append(QACheck('empty_csv', 'CRITICAL', state_code, f'CSV is empty'))
        return issues

    # Filter to monthly non-sport rows
    monthly = df[(df['period_type'] == 'monthly') & (df['sport_category'].isna() | (df['sport_category'] == ''))]
    if monthly.empty:
        monthly = df[df['sport_category'].isna() | (df['sport_category'] == '')]

    periods = sorted(monthly['period_end'].dropna().unique())
    if not periods:
        issues.append(QACheck('no_periods', 'CRITICAL', state_code, 'No period data found'))
        return issues

    latest_period = periods[-1]

    # --- Check: Staleness ---
    try:
        last_date = datetime.strptime(str(latest_period)[:10], '%Y-%m-%d').date()
        days_stale = (date.today() - last_date).days
        if days_stale > MAX_DAYS_STALE:
            issues.append(QACheck('stale_data', 'WARNING', state_code,
                f'Data is {days_stale} days old (latest: {latest_period})',
                value=days_stale, expected=f'<{MAX_DAYS_STALE}'))
    except Exception:
        pass

    # --- Check: Future dates ---
    future_cutoff = (date.today() + timedelta(days=31)).isoformat()
    future = monthly[monthly['period_end'] > future_cutoff]
    if len(future) > 0:
        issues.append(QACheck('future_dates', 'CRITICAL', state_code,
            f'{len(future)} rows with future dates (>{future_cutoff})',
            value=len(future)))

    # --- Latest period aggregation ---
    latest = monthly[monthly['period_end'] == latest_period]
    ops = latest[~latest['operator_standard'].isin(['TOTAL', 'ALL', 'UNKNOWN'])]
    totals = latest[latest['operator_standard'].isin(['TOTAL', 'ALL'])]
    use = ops if len(ops) > 0 else latest

    total_handle = use['handle'].sum()
    if total_handle == 0 and len(totals) > 0:
        total_handle = totals['handle'].sum()

    total_ggr = use['standard_ggr'].fillna(use['gross_revenue']).fillna(0).sum()
    if total_ggr == 0 and len(totals) > 0:
        total_ggr = totals['standard_ggr'].fillna(totals['gross_revenue']).fillna(0).sum()

    # --- Check: Handle range ---
    reports_handle = STATE_REGISTRY.get(state_code, {}).get('reports_handle', True)
    if reports_handle is not False and total_handle > 0:
        if total_handle < HANDLE_MIN:
            issues.append(QACheck('handle_too_low', 'CRITICAL', state_code,
                f'Handle {fmt(total_handle)} is suspiciously low',
                value=fmt(total_handle), expected=f'>={fmt(HANDLE_MIN)}'))
        if total_handle > HANDLE_MAX:
            issues.append(QACheck('handle_too_high', 'CRITICAL', state_code,
                f'Handle {fmt(total_handle)} is suspiciously high',
                value=fmt(total_handle), expected=f'<={fmt(HANDLE_MAX)}'))

    # --- Check: GGR range ---
    if total_ggr != 0:
        if abs(total_ggr) < GGR_MIN and total_handle > HANDLE_MIN:
            issues.append(QACheck('ggr_too_low', 'WARNING', state_code,
                f'GGR {fmt(total_ggr)} is very low relative to handle {fmt(total_handle)}',
                value=fmt(total_ggr)))
        if abs(total_ggr) > GGR_MAX:
            issues.append(QACheck('ggr_too_high', 'CRITICAL', state_code,
                f'GGR {fmt(total_ggr)} is suspiciously high',
                value=fmt(total_ggr), expected=f'<={fmt(GGR_MAX)}'))

    # --- Check: Hold % range (state total) ---
    if total_handle > 0 and total_ggr != 0:
        hold = total_ggr / total_handle
        if hold < HOLD_STATE_MIN:
            issues.append(QACheck('hold_too_low', 'WARNING', state_code,
                f'State hold {hold*100:.1f}% below {HOLD_STATE_MIN*100}%',
                value=f'{hold*100:.1f}%', expected=f'>={HOLD_STATE_MIN*100}%'))
        if hold > HOLD_STATE_MAX:
            issues.append(QACheck('hold_too_high', 'WARNING', state_code,
                f'State hold {hold*100:.1f}% above {HOLD_STATE_MAX*100}%',
                value=f'{hold*100:.1f}%', expected=f'<={HOLD_STATE_MAX*100}%'))

    # --- Check: GGR <= Handle ---
    if total_handle > 0 and total_ggr > total_handle:
        issues.append(QACheck('ggr_exceeds_handle', 'CRITICAL', state_code,
            f'GGR {fmt(total_ggr)} > Handle {fmt(total_handle)}',
            value=fmt(total_ggr), expected=f'<= {fmt(total_handle)}'))

    # --- Check: Zero handle + zero GGR ---
    if total_handle == 0 and total_ggr == 0 and reports_handle is not False:
        issues.append(QACheck('zero_data', 'CRITICAL', state_code,
            f'Both handle and GGR are $0 for {latest_period}'))

    # --- Check: Operator count ---
    n_ops = len(ops['operator_standard'].unique()) if len(ops) > 0 else 0
    if n_ops > MAX_OPERATORS:
        issues.append(QACheck('too_many_operators', 'WARNING', state_code,
            f'{n_ops} operators seems high (dedup issue?)',
            value=n_ops, expected=f'<={MAX_OPERATORS}'))

    # --- Check: Per-operator hold ---
    if len(ops) > 0:
        for _, op_row in ops.iterrows():
            op_handle = op_row.get('handle', 0) or 0
            op_ggr = op_row.get('standard_ggr') or op_row.get('gross_revenue') or 0
            if op_handle > 0 and op_ggr != 0:
                op_hold = op_ggr / op_handle
                op_name = op_row.get('operator_standard', '?')
                if op_hold > HOLD_OP_MAX:
                    issues.append(QACheck('operator_hold_high', 'WARNING', state_code,
                        f'{op_name} hold {op_hold*100:.1f}% > {HOLD_OP_MAX*100}%',
                        value=f'{op_hold*100:.1f}%'))
                if op_hold < HOLD_OP_MIN:
                    issues.append(QACheck('operator_hold_negative', 'WARNING', state_code,
                        f'{op_name} hold {op_hold*100:.1f}% < {HOLD_OP_MIN*100}%',
                        value=f'{op_hold*100:.1f}%'))

    # --- Check: MoM spike/drop ---
    if len(periods) >= 2:
        prev_period = periods[-2]
        prev = monthly[monthly['period_end'] == prev_period]
        prev_ops = prev[~prev['operator_standard'].isin(['TOTAL', 'ALL', 'UNKNOWN'])]
        prev_totals = prev[prev['operator_standard'].isin(['TOTAL', 'ALL'])]
        prev_use = prev_ops if len(prev_ops) > 0 else prev

        prev_handle = prev_use['handle'].sum()
        if prev_handle == 0 and len(prev_totals) > 0:
            prev_handle = prev_totals['handle'].sum()

        if prev_handle > 0 and total_handle > 0:
            ratio = total_handle / prev_handle
            if ratio > MOM_SPIKE:
                issues.append(QACheck('handle_mom_spike', 'CRITICAL', state_code,
                    f'Handle jumped {ratio:.1f}x vs prior month ({fmt(prev_handle)} -> {fmt(total_handle)})',
                    value=f'{ratio:.1f}x', expected=f'<{MOM_SPIKE}x'))
            if ratio < MOM_DROP:
                issues.append(QACheck('handle_mom_drop', 'CRITICAL', state_code,
                    f'Handle dropped to {ratio:.2f}x of prior month ({fmt(prev_handle)} -> {fmt(total_handle)})',
                    value=f'{ratio:.2f}x', expected=f'>{MOM_DROP}x'))

    return issues


def main():
    parser = argparse.ArgumentParser(description='QA Data Agent')
    parser.add_argument('--states', nargs='+', help='Specific states to check')
    parser.add_argument('--json', action='store_true', help='Output JSON report')
    args = parser.parse_args()

    states = [s.upper() for s in args.states] if args.states else sorted(STATE_REGISTRY.keys())

    all_issues = []
    for sc in states:
        issues = check_state(sc)
        all_issues.extend(issues)

    # Summarize
    critical = [i for i in all_issues if i.severity == 'CRITICAL']
    warnings = [i for i in all_issues if i.severity == 'WARNING']
    info = [i for i in all_issues if i.severity == 'INFO']

    report = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'states_checked': len(states),
        'total_issues': len(all_issues),
        'critical': len(critical),
        'warnings': len(warnings),
        'info': len(info),
        'passed': len(all_issues) == 0,
        'issues': [i.to_dict() for i in all_issues],
    }

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"QA DATA AGENT - {len(states)} states checked")
        print(f"{'=' * 60}")

        if not all_issues:
            print("ALL CHECKS PASSED")
        else:
            print(f"CRITICAL: {len(critical)}  |  WARNINGS: {len(warnings)}  |  INFO: {len(info)}")
            print()

            for severity in ['CRITICAL', 'WARNING']:
                items = [i for i in all_issues if i.severity == severity]
                if items:
                    print(f"--- {severity} ---")
                    for i in items:
                        val = f" [{i.value}]" if i.value else ""
                        print(f"  {i.state}: {i.message}{val}")
                    print()

    # Write report
    report_path = Path(__file__).parent / 'qa_report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    if not args.json:
        print(f"Report saved to {report_path}")

    # Exit code
    if critical:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

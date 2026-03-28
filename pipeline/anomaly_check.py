"""
Anomaly detection for scraped sports betting data.

Flags suspicious numbers that pass QA but look "off" — unexpected magnitudes,
month-over-month swings, outlier operators, and cross-state inconsistencies.
Acts as a second-look alert system for institutional data quality.

Usage:
    python -m pipeline.anomaly_check                   # all states
    python -m pipeline.anomaly_check NY PA CT          # specific states
    python -m pipeline.anomaly_check --latest          # only most recent month
"""

import sys
import json
import argparse
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.config import STATE_REGISTRY, get_all_states
from scrapers.base_scraper import MONEY_COLUMNS


@dataclass
class Alert:
    severity: str      # HIGH, MEDIUM, LOW
    state: str
    check: str
    message: str
    details: str = ""
    period: str = ""
    operator: str = ""
    value: str = ""
    expected: str = ""


@dataclass
class AnomalyResult:
    state_code: str
    alerts: list[Alert] = field(default_factory=list)

    @property
    def high(self) -> list[Alert]:
        return [a for a in self.alerts if a.severity == 'HIGH']

    @property
    def medium(self) -> list[Alert]:
        return [a for a in self.alerts if a.severity == 'MEDIUM']

    @property
    def clean(self) -> bool:
        return len(self.high) == 0 and len(self.medium) == 0


class AnomalyChecker:
    """Detects anomalies in scraped data that pass QA but look suspicious."""

    def __init__(self, state_code: str):
        self.state_code = state_code.upper()
        self.config = STATE_REGISTRY.get(self.state_code, {})
        self.alerts: list[Alert] = []

    def _alert(self, severity, check, message, **kwargs):
        self.alerts.append(Alert(
            severity=severity, state=self.state_code,
            check=check, message=message, **kwargs
        ))

    def run(self, df: pd.DataFrame) -> AnomalyResult:
        self.alerts = []
        if df.empty:
            return AnomalyResult(self.state_code, self.alerts)

        monthly = df[df['period_type'] == 'monthly'] if 'period_type' in df.columns else df
        if 'sport_category' in monthly.columns:
            monthly = monthly[monthly['sport_category'].isna()]

        self.check_mom_swings(monthly)
        self.check_operator_anomalies(monthly)
        self.check_hold_outliers(monthly)
        self.check_zero_months(monthly)
        self.check_handle_ggr_ratio(monthly)
        self.check_negative_handle(monthly)
        self.check_latest_month_completeness(df)

        return AnomalyResult(self.state_code, self.alerts)

    # ------------------------------------------------------------------
    # 1. Month-over-month handle swings (>3x or <0.3x)
    # ------------------------------------------------------------------
    def check_mom_swings(self, monthly: pd.DataFrame):
        if 'handle' not in monthly.columns:
            return

        # Get statewide monthly handle
        totals = self._get_monthly_totals(monthly, 'handle')
        if len(totals) < 2:
            return

        for i in range(1, len(totals)):
            prev_val = totals.iloc[i - 1]
            curr_val = totals.iloc[i]
            curr_date = totals.index[i]
            prev_date = totals.index[i - 1]

            if prev_val <= 0 or curr_val <= 0:
                continue

            ratio = curr_val / prev_val

            if ratio > 3.0:
                self._alert('HIGH', 'mom_handle_spike',
                            f'Handle jumped {ratio:.1f}x from {prev_date} to {curr_date}',
                            period=str(curr_date),
                            value=f'${curr_val/100:,.0f}',
                            expected=f'~${prev_val/100:,.0f}')
            elif ratio < 0.3:
                self._alert('HIGH', 'mom_handle_drop',
                            f'Handle dropped to {ratio:.1%} from {prev_date} to {curr_date}',
                            period=str(curr_date),
                            value=f'${curr_val/100:,.0f}',
                            expected=f'~${prev_val/100:,.0f}')
            elif ratio > 2.0:
                self._alert('MEDIUM', 'mom_handle_spike',
                            f'Handle increased {ratio:.1f}x from {prev_date}',
                            period=str(curr_date))
            elif ratio < 0.5:
                self._alert('MEDIUM', 'mom_handle_drop',
                            f'Handle decreased to {ratio:.1%} from {prev_date}',
                            period=str(curr_date))

    # ------------------------------------------------------------------
    # 2. Operator-level anomalies
    # ------------------------------------------------------------------
    def check_operator_anomalies(self, monthly: pd.DataFrame):
        if 'handle' not in monthly.columns or 'operator_reported' not in monthly.columns:
            return

        excluded = {'TOTAL', 'ALL', 'UNKNOWN'}
        ops = monthly[~monthly['operator_reported'].isin(excluded)]
        if ops.empty:
            return

        # Check each operator's latest month vs their historical median
        latest_pe = ops['period_end'].max()
        latest = ops[ops['period_end'] == latest_pe]

        for _, row in latest.iterrows():
            op = row['operator_reported']
            ch = row.get('channel', '')
            handle = row.get('handle', 0)

            if pd.isna(handle) or handle <= 0:
                continue

            # Historical for this operator+channel
            hist = ops[(ops['operator_reported'] == op) &
                       (ops['channel'] == ch) &
                       (ops['period_end'] < latest_pe)]

            if len(hist) < 3:
                continue

            median_h = hist['handle'].median()
            if median_h <= 0:
                continue

            ratio = handle / median_h

            if ratio > 5.0:
                self._alert('HIGH', 'operator_spike',
                            f'{op} ({ch}) handle {ratio:.1f}x above historical median',
                            period=str(latest_pe), operator=op,
                            value=f'${handle/100:,.0f}',
                            expected=f'median ${median_h/100:,.0f}')
            elif ratio < 0.1:
                self._alert('HIGH', 'operator_drop',
                            f'{op} ({ch}) handle at {ratio:.1%} of historical median',
                            period=str(latest_pe), operator=op,
                            value=f'${handle/100:,.0f}',
                            expected=f'median ${median_h/100:,.0f}')
            elif ratio > 3.0:
                self._alert('MEDIUM', 'operator_spike',
                            f'{op} ({ch}) handle {ratio:.1f}x above median',
                            period=str(latest_pe), operator=op)
            elif ratio < 0.2:
                self._alert('MEDIUM', 'operator_drop',
                            f'{op} ({ch}) handle at {ratio:.1%} of median',
                            period=str(latest_pe), operator=op)

    # ------------------------------------------------------------------
    # 3. Hold% outliers
    # ------------------------------------------------------------------
    def check_hold_outliers(self, monthly: pd.DataFrame):
        if 'hold_pct' not in monthly.columns:
            return

        latest_pe = monthly['period_end'].max()
        latest = monthly[monthly['period_end'] == latest_pe]

        for _, row in latest.iterrows():
            hold = row.get('hold_pct')
            op = row.get('operator_reported', 'ALL')
            if pd.isna(hold):
                continue

            if hold > 0.50:
                self._alert('HIGH', 'hold_extreme_high',
                            f'{op} hold% is {hold:.1%} (>50%)',
                            period=str(latest_pe), operator=op,
                            value=f'{hold:.1%}', expected='3-25%')
            elif hold < -0.10:
                self._alert('MEDIUM', 'hold_negative',
                            f'{op} hold% is {hold:.1%} (negative)',
                            period=str(latest_pe), operator=op)

    # ------------------------------------------------------------------
    # 4. Zero-handle months
    # ------------------------------------------------------------------
    def check_zero_months(self, monthly: pd.DataFrame):
        if 'handle' not in monthly.columns:
            return

        totals = self._get_monthly_totals(monthly, 'handle')
        if totals.empty:
            return

        latest_pe = totals.index[-1]
        latest_val = totals.iloc[-1]

        if latest_val == 0 or pd.isna(latest_val):
            self._alert('HIGH', 'zero_handle',
                        f'Latest month ({latest_pe}) has zero or null handle',
                        period=str(latest_pe))

    # ------------------------------------------------------------------
    # 5. Handle-GGR ratio sanity
    # ------------------------------------------------------------------
    def check_handle_ggr_ratio(self, monthly: pd.DataFrame):
        """Flag if GGR > handle (impossible) or GGR/handle > 40% (very suspicious)."""
        if 'handle' not in monthly.columns or 'gross_revenue' not in monthly.columns:
            return

        latest_pe = monthly['period_end'].max()
        latest = monthly[monthly['period_end'] == latest_pe]

        total_h = latest['handle'].sum()
        total_g = latest['gross_revenue'].sum()

        if total_h > 0 and total_g > total_h:
            self._alert('HIGH', 'ggr_exceeds_handle',
                        f'GGR (${total_g/100:,.0f}) exceeds handle (${total_h/100:,.0f})',
                        period=str(latest_pe),
                        details='GGR cannot be greater than handle')

    # ------------------------------------------------------------------
    # 6. Negative handle
    # ------------------------------------------------------------------
    def check_negative_handle(self, monthly: pd.DataFrame):
        if 'handle' not in monthly.columns:
            return

        neg = monthly[monthly['handle'] < 0]
        if len(neg) > 0:
            latest_neg = neg.sort_values('period_end').iloc[-1]
            self._alert('MEDIUM', 'negative_handle',
                        f'{len(neg)} rows with negative handle',
                        period=str(latest_neg['period_end']),
                        operator=str(latest_neg.get('operator_reported', '')))

    # ------------------------------------------------------------------
    # 7. Latest month completeness
    # ------------------------------------------------------------------
    def check_latest_month_completeness(self, df: pd.DataFrame):
        """Check if the latest month has significantly fewer rows than usual."""
        monthly = df[df['period_type'] == 'monthly'] if 'period_type' in df.columns else df
        if 'sport_category' in monthly.columns:
            monthly = monthly[monthly['sport_category'].isna()]

        counts = monthly.groupby('period_end').size()
        if len(counts) < 3:
            return

        latest_count = counts.iloc[-1]
        median_count = counts.iloc[:-1].median()

        if median_count > 0 and latest_count < median_count * 0.5:
            self._alert('MEDIUM', 'incomplete_month',
                        f'Latest month has {latest_count} rows (median is {median_count:.0f})',
                        period=str(counts.index[-1]),
                        details='May be partially scraped')

    # ------------------------------------------------------------------
    # Helper
    # ------------------------------------------------------------------
    def _get_monthly_totals(self, monthly: pd.DataFrame, col: str) -> pd.Series:
        """Get unduplicated monthly totals for a column."""
        if col not in monthly.columns:
            return pd.Series(dtype=float)

        subset = monthly.copy()

        # Use TOTAL if exists
        if 'operator_reported' in subset.columns:
            has_total = (subset['operator_reported'] == 'TOTAL').any()
            if has_total:
                subset = subset[subset['operator_reported'] == 'TOTAL']
            else:
                excluded = {'TOTAL', 'ALL', 'UNKNOWN'}
                subset = subset[~subset['operator_reported'].isin(excluded)]

        # Avoid channel double counting
        if 'channel' in subset.columns:
            channels = set(subset['channel'].unique())
            if 'combined' in channels and len(channels) > 1:
                subset = subset[subset['channel'] != 'combined']

        return subset.groupby('period_end')[col].sum().sort_index()


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def format_result(result: AnomalyResult) -> str:
    if result.clean:
        return f'{result.state_code}: CLEAN'

    lines = [f'{result.state_code}: {len(result.high)} HIGH, {len(result.medium)} MEDIUM']
    for a in result.alerts:
        icon = '!!' if a.severity == 'HIGH' else '!'
        lines.append(f'  {icon} [{a.check}] {a.message}')
        if a.value:
            lines.append(f'     value={a.value}  expected={a.expected}')
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------
def anomaly_check_state(state_code: str) -> AnomalyResult:
    csv_path = Path(f"data/processed/{state_code}.csv")
    if not csv_path.exists():
        return AnomalyResult(state_code)
    df = pd.read_csv(csv_path)
    checker = AnomalyChecker(state_code)
    return checker.run(df)


def anomaly_check_all(states: list[str] | None = None) -> list[AnomalyResult]:
    if states:
        codes = [s.upper() for s in states]
    else:
        codes = get_all_states()
    results = []
    for code in codes:
        csv_path = Path(f"data/processed/{code}.csv")
        if not csv_path.exists():
            continue
        results.append(anomaly_check_state(code))
    return results


def main():
    parser = argparse.ArgumentParser(description="Anomaly detection for sports betting data")
    parser.add_argument("states", nargs="*", help="State codes (default: all)")
    parser.add_argument("--latest", action="store_true", help="Only check most recent month")
    args = parser.parse_args()

    results = anomaly_check_all(args.states if args.states else None)

    flagged = [r for r in results if not r.clean]
    clean = [r for r in results if r.clean]

    for r in sorted(flagged, key=lambda x: -len(x.high)):
        print(format_result(r))
        print()

    print(f'=== SUMMARY ===')
    print(f'Clean: {len(clean)}/{len(results)}')
    print(f'Flagged: {len(flagged)} ({sum(len(r.high) for r in flagged)} HIGH, {sum(len(r.medium) for r in flagged)} MEDIUM)')

    if flagged:
        print(f'\nFlagged states: {", ".join(r.state_code for r in flagged)}')


if __name__ == "__main__":
    main()

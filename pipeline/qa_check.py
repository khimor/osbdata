"""
QA check pipeline — comprehensive data quality audit for institutional-grade
sports betting data.

Checks 7 dimensions: channel independence, operator completeness, sport
breakdown, financial integrity, temporal integrity, magnitude, and field
completeness. Produces a letter grade (A-F) per state.

Usage:
    python -m pipeline.qa_check                    # all states
    python -m pipeline.qa_check NY PA CT           # specific states
    python -m pipeline.qa_check --json             # JSON output
    python -m pipeline.qa_check --verbose          # detailed findings
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
from scrapers.base_scraper import STANDARD_COLUMNS, MONEY_COLUMNS


# ---------------------------------------------------------------------------
# Severity levels
# ---------------------------------------------------------------------------
class QALevel:
    FAIL = "FAIL"    # Blocking: data cannot be trusted
    WARN = "WARN"    # Non-blocking but needs investigation
    PASS = "PASS"    # Check passed
    SKIP = "SKIP"    # Check not applicable for this state


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class QAFinding:
    level: str
    category: str      # channels, operators, sports, financials, temporal, magnitude, completeness
    check: str         # specific check name
    message: str
    details: str = ""
    expected: str = ""
    actual: str = ""


@dataclass
class QAResult:
    state_code: str
    findings: list[QAFinding] = field(default_factory=list)
    score: str = ""
    summary_stats: dict = field(default_factory=dict)

    @property
    def fails(self) -> list[QAFinding]:
        return [f for f in self.findings if f.level == QALevel.FAIL]

    @property
    def warns(self) -> list[QAFinding]:
        return [f for f in self.findings if f.level == QALevel.WARN]

    @property
    def passes(self) -> list[QAFinding]:
        return [f for f in self.findings if f.level == QALevel.PASS]

    @property
    def skips(self) -> list[QAFinding]:
        return [f for f in self.findings if f.level == QALevel.SKIP]

    @property
    def passed(self) -> bool:
        return len(self.fails) == 0


# ---------------------------------------------------------------------------
# QA Checker
# ---------------------------------------------------------------------------
class QAChecker:

    # Tier magnitude ranges: (min, max) monthly handle in CENTS
    TIER_RANGES = {
        1: (30_000_000_000, 300_000_000_000),       # $300M - $3B
        2: (5_000_000_000, 100_000_000_000),         # $50M  - $1B
        3: (1_000_000_000, 50_000_000_000),          # $10M  - $500M
        4: (100_000_000, 20_000_000_000),            # $1M   - $200M
        5: (10_000_000, 50_000_000_000),             # $100K - $500M
    }

    def __init__(self, state_code: str):
        self.state_code = state_code.upper()
        if self.state_code not in STATE_REGISTRY:
            raise ValueError(f"Unknown state: {self.state_code}")
        self.config = STATE_REGISTRY[self.state_code]
        self.findings: list[QAFinding] = []

    def _add(self, level: str, category: str, check: str, message: str,
             details: str = "", expected: str = "", actual: str = ""):
        self.findings.append(QAFinding(level, category, check, message, details, expected, actual))

    def run(self, df: pd.DataFrame) -> QAResult:
        self.findings = []

        if df.empty:
            self._add(QALevel.FAIL, "general", "empty_data", "No data rows")
            return self._build_result()

        # Filter to monthly for most checks
        monthly = df[df['period_type'] == 'monthly'] if 'period_type' in df.columns else df

        self.check_channels(df, monthly)
        self.check_operators(df, monthly)
        self.check_sports(df, monthly)
        self.check_financials(df, monthly)
        self.check_temporal(df, monthly)
        self.check_magnitude(monthly)
        self.check_completeness(df)

        return self._build_result()

    def _build_result(self) -> QAResult:
        score = self._compute_score()
        summary = self._compute_summary_stats()
        return QAResult(self.state_code, list(self.findings), score, summary)

    def _compute_score(self) -> str:
        fails = sum(1 for f in self.findings if f.level == QALevel.FAIL)
        warns = sum(1 for f in self.findings if f.level == QALevel.WARN)
        if fails >= 2:
            return 'F'
        if fails == 1:
            return 'D'
        if warns >= 6:
            return 'D'
        if warns >= 3:
            return 'C'
        if warns >= 1:
            return 'B'
        return 'A'

    def _compute_summary_stats(self) -> dict:
        categories = ['channels', 'operators', 'sports', 'financials',
                       'temporal', 'magnitude', 'completeness']
        stats = {}
        for cat in categories:
            cat_findings = [f for f in self.findings if f.category == cat]
            stats[cat] = {
                'pass': sum(1 for f in cat_findings if f.level == QALevel.PASS),
                'warn': sum(1 for f in cat_findings if f.level == QALevel.WARN),
                'fail': sum(1 for f in cat_findings if f.level == QALevel.FAIL),
                'skip': sum(1 for f in cat_findings if f.level == QALevel.SKIP),
            }
        return stats

    # ------------------------------------------------------------------
    # Helper: get unduplicated monthly total handle
    # ------------------------------------------------------------------
    def _get_monthly_total_handle(self, monthly: pd.DataFrame) -> pd.Series:
        """Return Series indexed by period_end with unduplicated monthly handle."""
        if monthly.empty or 'handle' not in monthly.columns:
            return pd.Series(dtype=float)

        subset = monthly.copy()

        # Exclude sport breakdown rows
        if 'sport_category' in subset.columns:
            subset = subset[subset['sport_category'].isna()]

        if subset.empty:
            return pd.Series(dtype=float)

        # Operator layer: use TOTAL if it exists, else sum non-TOTAL
        if 'operator_standard' in subset.columns:
            has_total = (subset['operator_standard'] == 'TOTAL').any()
            if has_total:
                subset = subset[subset['operator_standard'] == 'TOTAL']
            else:
                subset = subset[~subset['operator_standard'].isin(['TOTAL'])]

        # Channel layer: per-period, use combined if available, else sum splits.
        # This handles states (e.g. CO) where some early months report combined
        # but most months report retail + online separately.
        if 'channel' in subset.columns:
            channels = set(subset['channel'].dropna().unique())
            has_combined = 'combined' in channels
            has_splits = 'online' in channels or 'retail' in channels

            if has_combined and has_splits:
                # Mixed: decide per-period
                results = {}
                for pe, grp in subset.groupby('period_end'):
                    pe_channels = set(grp['channel'].dropna().unique())
                    if 'combined' in pe_channels:
                        results[pe] = grp[grp['channel'] == 'combined']['handle'].sum()
                    else:
                        results[pe] = grp[grp['channel'].isin(['online', 'retail'])]['handle'].sum()
                return pd.Series(results, dtype=float)
            elif has_combined:
                subset = subset[subset['channel'] == 'combined']
            else:
                subset = subset[subset['channel'].isin(['online', 'retail'])]

        return subset.groupby('period_end')['handle'].sum()

    # ------------------------------------------------------------------
    # 1. Channel Independence
    # ------------------------------------------------------------------
    def check_channels(self, df: pd.DataFrame, monthly: pd.DataFrame):
        has_split = self.config.get('has_channel_split', False)
        channels_in_data = set(df['channel'].dropna().unique()) if 'channel' in df.columns else set()
        has_online = 'online' in channels_in_data
        has_retail = 'retail' in channels_in_data
        has_combined = 'combined' in channels_in_data

        # 1. Config-data alignment
        if has_split and not has_online and not has_retail:
            self._add(QALevel.WARN, 'channels', 'config_mismatch',
                      'Config says channel split available but data only has combined',
                      expected='online + retail', actual=str(channels_in_data))
        elif not has_split and has_online and has_retail:
            self._add(QALevel.PASS, 'channels', 'config_mismatch',
                      'Data has channel split beyond config expectation')
        else:
            self._add(QALevel.PASS, 'channels', 'config_mismatch',
                      'Channel config matches data')

        # 2. Retail + Online = Combined verification
        if has_combined and (has_online or has_retail) and 'handle' in monthly.columns:
            non_sport = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly
            mismatch_periods = 0
            total_periods = 0

            for pe in non_sport['period_end'].unique():
                pe_data = non_sport[non_sport['period_end'] == pe]
                combined_handle = pe_data[pe_data['channel'] == 'combined']['handle'].sum()
                split_handle = pe_data[pe_data['channel'].isin(['online', 'retail'])]['handle'].sum()

                if pd.notna(combined_handle) and combined_handle != 0 and split_handle != 0:
                    total_periods += 1
                    pct_diff = abs(split_handle - combined_handle) / max(abs(combined_handle), 1)
                    if pct_diff > 0.01:
                        mismatch_periods += 1

            if total_periods > 0 and mismatch_periods > 0:
                pct = mismatch_periods / total_periods
                self._add(QALevel.FAIL if pct > 0.2 else QALevel.WARN,
                          'channels', 'channel_sum',
                          f'retail+online != combined in {mismatch_periods}/{total_periods} periods',
                          expected='<1% difference', actual=f'{pct:.0%} of periods mismatch')
            elif total_periods > 0:
                self._add(QALevel.PASS, 'channels', 'channel_sum',
                          f'retail+online matches combined across {total_periods} periods')
            else:
                self._add(QALevel.SKIP, 'channels', 'channel_sum',
                          'No periods with both combined and split channel data')
        else:
            self._add(QALevel.SKIP, 'channels', 'channel_sum',
                      'N/A (no combined+split coexistence)')

        # 3. Channel magnitude ratio
        if has_online and has_retail and 'handle' in monthly.columns:
            non_sport = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly
            online_h = non_sport[non_sport['channel'] == 'online']['handle'].sum()
            retail_h = non_sport[non_sport['channel'] == 'retail']['handle'].sum()
            total = online_h + retail_h

            if total > 0:
                online_pct = online_h / total
                # Check if state is retail-only by design
                is_retail_only = self.config.get('online_tax_rate') is None and self.config.get('retail_tax_rate') is not None
                if online_pct < 0.50 and not is_retail_only:
                    self._add(QALevel.WARN, 'channels', 'channel_magnitude',
                              f'Online is only {online_pct:.0%} of total handle',
                              expected='>50% online for most markets',
                              actual=f'{online_pct:.0%}')
                else:
                    self._add(QALevel.PASS, 'channels', 'channel_magnitude',
                              f'Online is {online_pct:.0%} of total handle')
            else:
                self._add(QALevel.SKIP, 'channels', 'channel_magnitude', 'No handle data')
        else:
            self._add(QALevel.SKIP, 'channels', 'channel_magnitude',
                      'N/A (single channel)')

        # 4. Data leakage detection
        if has_online and has_retail and 'handle' in monthly.columns:
            non_sport = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly
            check_cols = [c for c in MONEY_COLUMNS if c in non_sport.columns and non_sport[c].notna().any()]
            identical_count = 0
            comparable_count = 0

            for pe in non_sport['period_end'].unique():
                pe_data = non_sport[non_sport['period_end'] == pe]
                online_rows = pe_data[pe_data['channel'] == 'online']
                retail_rows = pe_data[pe_data['channel'] == 'retail']

                if online_rows.empty or retail_rows.empty:
                    continue
                comparable_count += 1

                matches = 0
                for col in check_cols:
                    o_val = online_rows[col].sum()
                    r_val = retail_rows[col].sum()
                    if pd.notna(o_val) and pd.notna(r_val) and o_val != 0 and o_val == r_val:
                        matches += 1
                if matches >= 3:
                    identical_count += 1

            if comparable_count > 0 and identical_count / comparable_count > 0.5:
                self._add(QALevel.WARN, 'channels', 'data_leakage',
                          f'Online and retail have identical values in {identical_count}/{comparable_count} periods',
                          details='Possible copy-paste between channels')
            elif comparable_count > 0:
                self._add(QALevel.PASS, 'channels', 'data_leakage',
                          'No data leakage between channels')
            else:
                self._add(QALevel.SKIP, 'channels', 'data_leakage',
                          'Not enough comparable periods')
        else:
            self._add(QALevel.SKIP, 'channels', 'data_leakage', 'N/A (single channel)')

    # ------------------------------------------------------------------
    # 2. Operator Completeness
    # ------------------------------------------------------------------
    def check_operators(self, df: pd.DataFrame, monthly: pd.DataFrame):
        has_ops = self.config.get('has_operator_breakdown', False)
        if not has_ops:
            self._add(QALevel.SKIP, 'operators', 'all',
                      'N/A (no operator breakdown for this state)')
            return

        if 'operator_standard' not in df.columns:
            self._add(QALevel.FAIL, 'operators', 'operators_exist',
                      'No operator_standard column')
            return

        excluded = {'TOTAL', 'ALL', 'UNKNOWN'}
        # Use operator_reported for venue-level checks, fall back to operator_standard
        op_col = 'operator_reported' if 'operator_reported' in df.columns else 'operator_standard'
        non_total = df[~df[op_col].isin(excluded)]
        real_ops = non_total[op_col].dropna().unique()

        # 1. Operators exist
        if len(real_ops) == 0:
            self._add(QALevel.FAIL, 'operators', 'operators_exist',
                      'Config says operator breakdown available but no operator rows found',
                      expected='>0 operators', actual='0')
            return
        self._add(QALevel.PASS, 'operators', 'operators_exist',
                  f'{len(real_ops)} operators found')

        # 2. Operator count stability (use operator_reported for venue-level granularity)
        non_sport = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly
        non_total_monthly = non_sport[~non_sport[op_col].isin(excluded)]

        if not non_total_monthly.empty:
            per_period = non_total_monthly.groupby('period_end')[op_col].nunique()
            if len(per_period) >= 3:
                recent = per_period.tail(6)
                expected_count = recent.mode().iloc[0] if not recent.mode().empty else recent.median()
                latest_count = per_period.iloc[-1]

                if expected_count > 0 and latest_count < expected_count * 0.6:
                    self._add(QALevel.FAIL, 'operators', 'operator_stability',
                              f'Latest month has {latest_count} operators (expected ~{expected_count:.0f})',
                              expected=f'~{expected_count:.0f}', actual=str(latest_count),
                              details='Most recent report may be incomplete')
                else:
                    low_months = (per_period < expected_count * 0.6).sum()
                    if low_months > 0:
                        self._add(QALevel.WARN, 'operators', 'operator_stability',
                                  f'{low_months} months with <60% of expected operator count',
                                  expected=f'~{expected_count:.0f} per month',
                                  actual=f'{low_months} months below threshold')
                    else:
                        self._add(QALevel.PASS, 'operators', 'operator_stability',
                                  f'Operator count stable (~{expected_count:.0f} per month)')
            else:
                self._add(QALevel.SKIP, 'operators', 'operator_stability',
                          'Not enough periods for stability check')
        else:
            self._add(QALevel.SKIP, 'operators', 'operator_stability', 'No monthly operator data')

        # 3. SUM(operators) vs TOTAL row
        # Use operator_reported for TOTAL detection (both columns have 'TOTAL' for total rows)
        has_total_rows = (non_sport[op_col] == 'TOTAL').any() if op_col in non_sport.columns else False
        if has_total_rows and 'handle' in non_sport.columns:
            mismatch_count = 0
            total_checked = 0

            for pe in non_sport['period_end'].unique():
                pe_data = non_sport[non_sport['period_end'] == pe]
                # Group by channel to avoid cross-channel comparison
                for ch in pe_data['channel'].unique():
                    ch_data = pe_data[pe_data['channel'] == ch]
                    total_row = ch_data[ch_data[op_col] == 'TOTAL']
                    op_rows = ch_data[~ch_data[op_col].isin(excluded)]

                    if total_row.empty or op_rows.empty:
                        continue

                    t_handle = total_row['handle'].sum()
                    o_handle = op_rows['handle'].sum()

                    if pd.notna(t_handle) and t_handle != 0:
                        total_checked += 1
                        pct_diff = abs(o_handle - t_handle) / abs(t_handle)
                        if pct_diff > 0.01:
                            mismatch_count += 1

            if total_checked > 0:
                mismatch_pct = mismatch_count / total_checked
                if mismatch_pct > 0.2:
                    self._add(QALevel.WARN, 'operators', 'sum_to_total',
                              f'Operator sum != TOTAL in {mismatch_count}/{total_checked} periods',
                              expected='<1% difference',
                              actual=f'{mismatch_pct:.0%} of periods mismatch')
                else:
                    self._add(QALevel.PASS, 'operators', 'sum_to_total',
                              f'Operator sum matches TOTAL across {total_checked} periods')
            else:
                self._add(QALevel.SKIP, 'operators', 'sum_to_total',
                          'No comparable TOTAL+operator periods')
        else:
            self._add(QALevel.SKIP, 'operators', 'sum_to_total', 'No TOTAL rows')

        # 4. Parent company populated
        if 'parent_company' in non_total.columns:
            null_parent = non_total[non_total['parent_company'].isna()]
            if len(null_parent) > 0:
                ops_missing = null_parent[op_col].unique()
                self._add(QALevel.WARN, 'operators', 'parent_company',
                          f'{len(ops_missing)} operators missing parent_company',
                          details=', '.join(list(ops_missing)[:5]))
            else:
                self._add(QALevel.PASS, 'operators', 'parent_company',
                          'All operators have parent_company')
        else:
            self._add(QALevel.SKIP, 'operators', 'parent_company',
                      'No parent_company column')

    # ------------------------------------------------------------------
    # 3. Sport Breakdown
    # ------------------------------------------------------------------
    def check_sports(self, df: pd.DataFrame, monthly: pd.DataFrame):
        has_sports = self.config.get('has_sport_breakdown', False)
        if not has_sports:
            self._add(QALevel.SKIP, 'sports', 'all',
                      'N/A (no sport breakdown for this state)')
            return

        sport_rows = df[df['sport_category'].notna()] if 'sport_category' in df.columns else pd.DataFrame()

        # 1. Sport rows exist
        if sport_rows.empty:
            self._add(QALevel.WARN, 'sports', 'sports_exist',
                      'Config says sport breakdown available but no sport data found')
            return

        sports_found = sorted(sport_rows['sport_category'].unique())
        self._add(QALevel.PASS, 'sports', 'sports_exist',
                  f'{len(sports_found)} sport categories found',
                  details=', '.join(sports_found[:10]))

        # 2. Expected categories
        expected_sports = self.config.get('sport_categories', [])
        if expected_sports:
            expected_set = {s.lower() for s in expected_sports}
            actual_set = {s.lower() for s in sports_found}
            missing = expected_set - actual_set
            if missing:
                self._add(QALevel.WARN, 'sports', 'expected_categories',
                          f'Missing expected sport categories: {missing}')
            else:
                self._add(QALevel.PASS, 'sports', 'expected_categories',
                          'All expected sport categories present')
        else:
            self._add(QALevel.SKIP, 'sports', 'expected_categories',
                      'No expected sport_categories in config')

        # 3. Sport-channel intersection
        has_channel_split = self.config.get('has_channel_split', False)
        sport_channels = set(sport_rows['channel'].dropna().unique()) if 'channel' in sport_rows.columns else set()

        if has_channel_split:
            has_sport_split = 'online' in sport_channels or 'retail' in sport_channels
            if not has_sport_split:
                self._add(QALevel.WARN, 'sports', 'sport_channel_split',
                          'State has channel split but sport data lacks per-channel breakdown',
                          expected='online + retail sport rows',
                          actual=str(sport_channels))
            else:
                self._add(QALevel.PASS, 'sports', 'sport_channel_split',
                          'Sport data has per-channel breakdown')
        else:
            self._add(QALevel.SKIP, 'sports', 'sport_channel_split',
                      'N/A (no channel split for this state)')

        # 4. Sport handle sum vs total
        if 'handle' in sport_rows.columns and 'handle' in monthly.columns:
            total_handle = self._get_monthly_total_handle(monthly)
            sport_monthly = sport_rows[sport_rows['period_type'] == 'monthly'] if 'period_type' in sport_rows.columns else sport_rows

            # Sum sport handles per period (avoid double-counting channels)
            if 'channel' in sport_monthly.columns:
                sport_ch = set(sport_monthly['channel'].dropna().unique())
                if 'combined' in sport_ch and len(sport_ch) > 1:
                    sport_monthly = sport_monthly[sport_monthly['channel'] == 'combined']
                elif 'combined' not in sport_ch:
                    sport_monthly = sport_monthly[sport_monthly['channel'].isin(['online', 'retail'])]

            sport_sum = sport_monthly.groupby('period_end')['handle'].sum()
            common_periods = total_handle.index.intersection(sport_sum.index)

            if len(common_periods) > 0:
                overcount = 0
                for pe in common_periods:
                    t = total_handle[pe]
                    s = sport_sum[pe]
                    if pd.notna(t) and t > 0 and pd.notna(s) and s > t * 1.1:
                        overcount += 1

                if overcount > 0:
                    self._add(QALevel.WARN, 'sports', 'sport_handle_sum',
                              f'Sport handle sum exceeds total handle in {overcount}/{len(common_periods)} periods',
                              details='Possible double counting in sport breakdown')
                else:
                    self._add(QALevel.PASS, 'sports', 'sport_handle_sum',
                              'Sport handle sum within expected range')
            else:
                self._add(QALevel.SKIP, 'sports', 'sport_handle_sum',
                          'No comparable periods between sport and total data')
        else:
            self._add(QALevel.SKIP, 'sports', 'sport_handle_sum', 'No handle data')

    # ------------------------------------------------------------------
    # 4. Financial Integrity
    # ------------------------------------------------------------------
    def check_financials(self, df: pd.DataFrame, monthly: pd.DataFrame):
        # Exclude sport breakdown rows for financial checks
        main = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly

        # 1. standard_ggr coverage
        if 'handle' in main.columns and 'payouts' in main.columns:
            has_both = main['handle'].notna() & main['payouts'].notna()
            if has_both.any():
                ggr_null = main.loc[has_both, 'standard_ggr'].isna() if 'standard_ggr' in main.columns else has_both
                null_pct = ggr_null.sum() / has_both.sum()
                if null_pct > 0.1:
                    self._add(QALevel.WARN, 'financials', 'standard_ggr_coverage',
                              f'standard_ggr null in {null_pct:.0%} of rows where handle+payouts exist',
                              expected='<10% null', actual=f'{null_pct:.0%} null')
                else:
                    self._add(QALevel.PASS, 'financials', 'standard_ggr_coverage',
                              f'standard_ggr populated ({1-null_pct:.0%} coverage)')
            else:
                self._add(QALevel.SKIP, 'financials', 'standard_ggr_coverage',
                          'No rows with both handle and payouts')
        else:
            self._add(QALevel.SKIP, 'financials', 'standard_ggr_coverage',
                      'Missing handle or payouts column')

        # 2. Median hold% check
        if 'hold_pct' in main.columns:
            valid_hold = main['hold_pct'].dropna()
            valid_hold = valid_hold[(valid_hold > 0) & (valid_hold < 1)]
            if len(valid_hold) >= 5:
                median_hold = valid_hold.median()
                if median_hold < 0.03:
                    self._add(QALevel.WARN, 'financials', 'median_hold',
                              f'Median hold% is {median_hold:.1%} (unusually low)',
                              expected='3-20%', actual=f'{median_hold:.1%}')
                elif median_hold > 0.20:
                    self._add(QALevel.WARN, 'financials', 'median_hold',
                              f'Median hold% is {median_hold:.1%} (unusually high)',
                              expected='3-20%', actual=f'{median_hold:.1%}')
                else:
                    self._add(QALevel.PASS, 'financials', 'median_hold',
                              f'Median hold% is {median_hold:.1%}')
            else:
                self._add(QALevel.SKIP, 'financials', 'median_hold',
                          'Not enough valid hold% values')
        else:
            self._add(QALevel.SKIP, 'financials', 'median_hold', 'No hold_pct column')

        # 3. Revenue sign consistency
        if 'gross_revenue' in main.columns and 'handle' in main.columns:
            has_handle = main['handle'].notna() & (main['handle'] > 0)
            neg_rev = has_handle & main['gross_revenue'].notna() & (main['gross_revenue'] < 0)
            if has_handle.sum() > 0:
                neg_pct = neg_rev.sum() / has_handle.sum()
                if neg_pct > 0.10:
                    self._add(QALevel.WARN, 'financials', 'revenue_sign',
                              f'Negative gross_revenue in {neg_pct:.0%} of rows with positive handle',
                              expected='<10%', actual=f'{neg_pct:.0%}')
                else:
                    self._add(QALevel.PASS, 'financials', 'revenue_sign',
                              'Revenue sign consistency OK')
            else:
                self._add(QALevel.SKIP, 'financials', 'revenue_sign', 'No positive handle rows')
        else:
            self._add(QALevel.SKIP, 'financials', 'revenue_sign',
                      'Missing gross_revenue or handle')

    # ------------------------------------------------------------------
    # 5. Temporal Integrity
    # ------------------------------------------------------------------
    def check_temporal(self, df: pd.DataFrame, monthly: pd.DataFrame):
        if 'period_end' not in df.columns:
            self._add(QALevel.FAIL, 'temporal', 'all', 'No period_end column')
            return

        dates = pd.to_datetime(df['period_end'])
        today = pd.Timestamp.now()

        # 1. Future dates
        future = dates[dates > today]
        if len(future) > 0:
            self._add(QALevel.FAIL, 'temporal', 'future_dates',
                      f'{len(future)} rows with future dates',
                      actual=str(future.max().date()))
        else:
            self._add(QALevel.PASS, 'temporal', 'future_dates', 'No future dates')

        # 2. Pre-launch dates
        launch_str = self.config.get('launch_date')
        if launch_str:
            launch = pd.Timestamp(launch_str)
            pre_launch = dates[dates < launch]
            if len(pre_launch) > 0:
                self._add(QALevel.WARN, 'temporal', 'pre_launch',
                          f'{len(pre_launch)} rows before launch date {launch_str}',
                          expected=f'>= {launch_str}',
                          actual=str(pre_launch.min().date()))
            else:
                self._add(QALevel.PASS, 'temporal', 'pre_launch', 'All dates after launch')
        else:
            self._add(QALevel.SKIP, 'temporal', 'pre_launch', 'No launch_date in config')

        # 3. Date gaps (monthly only)
        main = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly
        monthly_only = main[main['period_type'] == 'monthly'] if 'period_type' in main.columns else main
        if not monthly_only.empty:
            months = sorted(set(pd.to_datetime(monthly_only['period_end']).dt.to_period('M')))
            if len(months) >= 2:
                expected_range = pd.period_range(months[0], months[-1], freq='M')
                missing = set(expected_range) - set(months)
                if missing:
                    self._add(QALevel.WARN, 'temporal', 'date_gaps',
                              f'{len(missing)} missing months in reporting sequence',
                              details=', '.join(str(m) for m in sorted(missing)[:5]))
                else:
                    self._add(QALevel.PASS, 'temporal', 'date_gaps',
                              f'No gaps across {len(months)} months')
            else:
                self._add(QALevel.SKIP, 'temporal', 'date_gaps',
                          'Not enough monthly periods for gap check')
        else:
            self._add(QALevel.SKIP, 'temporal', 'date_gaps', 'No monthly data')

        # 4. Stale data (use operator_reported for venue-level granularity)
        op_col = 'operator_reported' if 'operator_reported' in monthly.columns else 'operator_standard'
        if 'handle' in monthly.columns and op_col in monthly.columns:
            main_monthly = monthly[monthly['sport_category'].isna()] if 'sport_category' in monthly.columns else monthly
            stale_ops = []
            for op in main_monthly[op_col].unique():
                op_data = main_monthly[main_monthly[op_col] == op].sort_values('period_end')
                handles = op_data['handle'].dropna().values
                if len(handles) >= 4:
                    # Check for 4+ consecutive identical values
                    for i in range(len(handles) - 3):
                        if len(set(handles[i:i+4])) == 1 and handles[i] != 0:
                            stale_ops.append(op)
                            break

            if stale_ops:
                self._add(QALevel.WARN, 'temporal', 'stale_data',
                          f'{len(stale_ops)} operators have identical handle 4+ consecutive months',
                          details=', '.join(stale_ops[:5]))
            else:
                self._add(QALevel.PASS, 'temporal', 'stale_data', 'No stale data detected')
        else:
            self._add(QALevel.SKIP, 'temporal', 'stale_data',
                      'Missing handle or operator data')

    # ------------------------------------------------------------------
    # 6. Magnitude
    # ------------------------------------------------------------------
    def check_magnitude(self, monthly: pd.DataFrame):
        tier = self.config.get('tier', 5)
        tier_min, tier_max = self.TIER_RANGES.get(tier, (0, float('inf')))

        total_handle = self._get_monthly_total_handle(monthly)
        valid = total_handle[total_handle.notna() & (total_handle > 0)]

        if len(valid) < 1:
            self._add(QALevel.SKIP, 'magnitude', 'handle_magnitude',
                      'No valid monthly handle data')
            return

        median_handle = valid.median()

        if median_handle > tier_max:
            self._add(QALevel.FAIL, 'magnitude', 'handle_magnitude',
                      f'Median monthly handle ${median_handle/100:,.0f} above Tier {tier} max',
                      expected=f'${tier_min/100:,.0f} - ${tier_max/100:,.0f}',
                      actual=f'${median_handle/100:,.0f}',
                      details='Possible unit error (values too large)')
        elif median_handle < tier_min:
            self._add(QALevel.WARN, 'magnitude', 'handle_magnitude',
                      f'Median monthly handle ${median_handle/100:,.0f} below Tier {tier} min',
                      expected=f'${tier_min/100:,.0f} - ${tier_max/100:,.0f}',
                      actual=f'${median_handle/100:,.0f}',
                      details='Handle lower than expected for tier')
        else:
            self._add(QALevel.PASS, 'magnitude', 'handle_magnitude',
                      f'Median monthly handle ${median_handle/100:,.0f} within Tier {tier} range')

    # ------------------------------------------------------------------
    # 7. Completeness
    # ------------------------------------------------------------------
    def check_completeness(self, df: pd.DataFrame):
        populated_cols = []
        sparse_cols = []
        empty_cols = []

        for col in MONEY_COLUMNS:
            if col not in df.columns:
                empty_cols.append(col)
                continue
            non_null = df[col].notna().sum()
            pct = non_null / len(df) if len(df) > 0 else 0
            if pct >= 0.5:
                populated_cols.append(col)
            elif pct >= 0.1:
                sparse_cols.append(col)
            else:
                empty_cols.append(col)

        # Warn for expected fields that are empty
        expected_fields = self._get_expected_fields()
        missing_expected = [f for f in expected_fields if f in empty_cols or f in sparse_cols]

        if missing_expected:
            self._add(QALevel.WARN, 'completeness', 'field_coverage',
                      f'{len(missing_expected)} expected fields at <50% coverage: {", ".join(missing_expected)}',
                      expected='All expected fields >50% populated',
                      actual=f'{len(populated_cols)}/{len(MONEY_COLUMNS)} at >50%')
        else:
            self._add(QALevel.PASS, 'completeness', 'field_coverage',
                      f'{len(populated_cols)}/{len(MONEY_COLUMNS)} money fields at >50% coverage')

        # Completeness grade
        n = len(populated_cols)
        if n >= 7:
            grade = 'A'
        elif n >= 5:
            grade = 'B'
        elif n >= 3:
            grade = 'C'
        elif n >= 1:
            grade = 'D'
        else:
            grade = 'F'

        self._add(QALevel.PASS, 'completeness', 'completeness_grade',
                  f'Data richness: {grade} ({n}/8 money fields populated)',
                  details=f'Populated: {", ".join(populated_cols) or "none"}')

    def _get_expected_fields(self) -> list[str]:
        """Determine which money fields should be populated based on config.

        Only expects fields that the state's regulatory reports actually contain.
        Revenue-share and lottery-model states often don't publish handle or gross_revenue.
        """
        expected = []
        tax_basis = self.config.get('tax_basis', '')
        fmt = self.config.get('format', '')
        freq = self.config.get('frequency', '')

        # Revenue-share / lottery models (DE, MT, NH, OR, RI, WV) often lack raw handle/GGR
        is_revenue_share = 'revenue_share' in tax_basis

        # Annual-only states have very sparse data
        if freq == 'annual':
            return []

        # Handle: most states report it, but some (NJ, IL) only report revenue
        # States with reports_handle=False in config explicitly don't publish handle
        reports_handle = self.config.get('reports_handle', True)
        if not is_revenue_share and reports_handle:
            expected.append('handle')

        # Gross revenue: most states have it in some form
        expected.append('gross_revenue')

        return expected


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------
CATEGORY_ORDER = ['channels', 'operators', 'sports', 'financials',
                  'temporal', 'magnitude', 'completeness']


def _cat_icon(stats: dict) -> str:
    if stats.get('fail', 0) > 0:
        return '\u2717'   # ✗
    if stats.get('warn', 0) > 0:
        return '\u26a0'   # ⚠
    if stats.get('pass', 0) > 0:
        return '\u2713'   # ✓
    return '\u2500'       # ─


def _cat_summary(category: str, findings: list[QAFinding]) -> str:
    """One-line summary for a category."""
    cat_f = [f for f in findings if f.category == category]
    if not cat_f:
        return 'N/A'

    # If all skipped
    if all(f.level == QALevel.SKIP for f in cat_f):
        msgs = [f.message for f in cat_f]
        return msgs[0] if msgs else 'N/A'

    # Prioritize fails, then warns, then pass summaries
    fails = [f for f in cat_f if f.level == QALevel.FAIL]
    warns = [f for f in cat_f if f.level == QALevel.WARN]

    if fails:
        return fails[0].message
    if warns:
        return warns[0].message

    passes = [f for f in cat_f if f.level == QALevel.PASS]
    if passes:
        return passes[0].message
    return 'OK'


def format_result(result: QAResult) -> str:
    lines = []
    p = len(result.passes)
    w = len(result.warns)
    f = len(result.fails)
    lines.append(f"{result.state_code} \u2500\u2500 {result.score} ({p} pass, {w} warn, {f} fail)")

    for cat in CATEGORY_ORDER:
        stats = result.summary_stats.get(cat, {})
        icon = _cat_icon(stats)
        summary = _cat_summary(cat, result.findings)
        lines.append(f"  {icon} {cat}: {summary}")

    return '\n'.join(lines)


def format_result_verbose(result: QAResult) -> str:
    lines = [format_result(result), '']
    for cat in CATEGORY_ORDER:
        cat_findings = [f for f in result.findings if f.category == cat]
        if not cat_findings:
            continue
        for f in cat_findings:
            icon = {'FAIL': '\u2717', 'WARN': '\u26a0', 'PASS': '\u2713', 'SKIP': '\u2500'}[f.level]
            lines.append(f"  {icon} [{f.level:4s}] {f.category}.{f.check}: {f.message}")
            if f.expected:
                lines.append(f"           expected: {f.expected}")
            if f.actual:
                lines.append(f"           actual:   {f.actual}")
            if f.details:
                lines.append(f"           details:  {f.details}")
    return '\n'.join(lines)


def format_summary(results: list[QAResult]) -> str:
    lines = []
    lines.append('=' * 56)
    lines.append('  QA CHECK SUMMARY')
    lines.append('=' * 56)

    grades = {}
    for r in results:
        grades[r.score] = grades.get(r.score, 0) + 1
    grade_str = '  |  '.join(f'{g}: {grades.get(g, 0)}' for g in ['A', 'B', 'C', 'D', 'F'])
    lines.append(f'  Total: {len(results)}  |  {grade_str}')
    lines.append('')
    lines.append(f'  {"State":<6s} {"Score":<6s} {"Pass":<6s} {"Warn":<6s} {"Fail":<6s} {"Skip":<6s}')
    lines.append(f'  {"─"*6} {"─"*5} {"─"*5} {"─"*5} {"─"*5} {"─"*5}')

    for r in sorted(results, key=lambda x: x.state_code):
        lines.append(f'  {r.state_code:<6s} {r.score:<6s} {len(r.passes):<6d} '
                     f'{len(r.warns):<6d} {len(r.fails):<6d} {len(r.skips):<6d}')

    return '\n'.join(lines)


def result_to_json(result: QAResult) -> dict:
    return {
        'state_code': result.state_code,
        'score': result.score,
        'passed': result.passed,
        'findings': [
            {
                'level': f.level,
                'category': f.category,
                'check': f.check,
                'message': f.message,
                'details': f.details,
                'expected': f.expected,
                'actual': f.actual,
            }
            for f in result.findings
        ],
        'summary_stats': result.summary_stats,
    }


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------
def qa_check_state(state_code: str) -> QAResult:
    state_code = state_code.upper()
    csv_path = Path(f"data/processed/{state_code}.csv")
    if not csv_path.exists():
        checker = QAChecker(state_code)
        checker._add(QALevel.FAIL, 'general', 'no_file',
                      f'No processed CSV at {csv_path}')
        return checker._build_result()
    df = pd.read_csv(csv_path)
    checker = QAChecker(state_code)
    return checker.run(df)


def qa_check_all(states: list[str] | None = None) -> list[QAResult]:
    if states:
        codes = [s.upper() for s in states]
    else:
        codes = get_all_states()
    results = []
    for code in codes:
        csv_path = Path(f"data/processed/{code}.csv")
        if not csv_path.exists():
            continue
        results.append(qa_check_state(code))
    return results


def main():
    parser = argparse.ArgumentParser(description="QA check state sports betting data")
    parser.add_argument("states", nargs="*", help="State codes (default: all)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all findings")
    args = parser.parse_args()

    results = qa_check_all(args.states if args.states else None)

    if not results:
        print("No processed state data found.")
        return

    if args.json:
        print(json.dumps([result_to_json(r) for r in results], indent=2))
        return

    for r in sorted(results, key=lambda x: x.state_code):
        if args.verbose:
            print(format_result_verbose(r))
        else:
            print(format_result(r))
        print()

    if len(results) > 1:
        print(format_summary(results))


if __name__ == "__main__":
    main()

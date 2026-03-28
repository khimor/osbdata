"""
Sense-check pipeline — catches scraper bugs that produce structurally valid
but numerically wrong data.

Checks for: unit errors, column swaps, YTD-as-monthly, double counting,
stale/repeated data, impossible magnitude shifts, and cross-field violations.

Usage:
    python3 -m pipeline.sense_check              # all states
    python3 -m pipeline.sense_check NY PA CT     # specific states
"""

import sys
import argparse
from pathlib import Path
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.config import STATE_REGISTRY
from scrapers.base_scraper import MONEY_COLUMNS


# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------
class Level:
    SUSPECT = "SUSPECT"    # Likely a real bug
    REVIEW = "REVIEW"      # Needs human review
    OK = "OK"              # Passed


@dataclass
class Finding:
    level: str
    check: str
    message: str
    details: str = ""


@dataclass
class SenseCheckResult:
    state_code: str
    findings: list[Finding] = field(default_factory=list)

    @property
    def suspects(self) -> list[Finding]:
        return [f for f in self.findings if f.level == Level.SUSPECT]

    @property
    def reviews(self) -> list[Finding]:
        return [f for f in self.findings if f.level == Level.REVIEW]

    @property
    def clean(self) -> bool:
        return len(self.suspects) == 0


# ---------------------------------------------------------------------------
# Sense Checker
# ---------------------------------------------------------------------------
class SenseChecker:
    def __init__(self, state_code: str):
        self.state_code = state_code.upper()
        self.config = STATE_REGISTRY.get(self.state_code, {})
        self.findings: list[Finding] = []

    def _add(self, level: str, check: str, msg: str, details: str = ""):
        self.findings.append(Finding(level, check, msg, details))

    def run(self, df: pd.DataFrame) -> SenseCheckResult:
        self.findings = []

        if df.empty:
            self._add(Level.SUSPECT, "empty", "No data")
            return SenseCheckResult(self.state_code, self.findings)

        self._check_column_swap(df)
        self._check_unit_errors(df)
        self._check_ytd_pattern(df)
        self._check_month_over_month_spikes(df)
        self._check_stale_data(df)
        self._check_double_counting(df)
        self._check_cross_field_sanity(df)
        self._check_operator_count_stability(df)
        self._check_hold_stability(df)

        return SenseCheckResult(self.state_code, self.findings)

    # ------------------------------------------------------------------
    # 1. Column swap / duplication
    # ------------------------------------------------------------------
    def _check_column_swap(self, df: pd.DataFrame):
        """Detect if handle == gross_revenue (column duplication) or handle < gross_revenue (swap)."""
        if 'handle' not in df.columns or 'gross_revenue' not in df.columns:
            return
        mask = df['handle'].notna() & df['gross_revenue'].notna() & (df['handle'] != 0)
        sub = df[mask]
        if sub.empty:
            return

        # Check for duplication: handle == gross_revenue in most rows
        equal = (sub['handle'] == sub['gross_revenue']).sum()
        pct_equal = equal / len(sub)
        if pct_equal > 0.8:
            self._add(Level.SUSPECT, "column_swap",
                      f"handle == gross_revenue in {pct_equal:.0%} of rows ({equal}/{len(sub)})",
                      "Scraper likely putting the same value in both columns")
            return

        # Check for swap: gross_revenue > handle consistently
        gr_exceeds = (sub['gross_revenue'].astype(float) > sub['handle'].astype(float)).sum()
        pct_exceeds = gr_exceeds / len(sub)
        if pct_exceeds > 0.3:
            self._add(Level.SUSPECT, "column_swap",
                      f"gross_revenue > handle in {pct_exceeds:.0%} of rows ({gr_exceeds}/{len(sub)})",
                      "Columns may be swapped — GGR should always be less than handle")

    # ------------------------------------------------------------------
    # 2. Unit / magnitude errors
    # ------------------------------------------------------------------
    def _check_unit_errors(self, df: pd.DataFrame):
        """Detect values off by 100x or 1000x from expected magnitudes."""
        tier = self.config.get('tier', 99)
        if 'handle' not in df.columns:
            return

        monthly = df[df['period_type'] == 'monthly']
        if monthly.empty:
            return

        # Expected monthly handle range per tier (in cents)
        expected = {
            1: (100_000_000_00, 30_000_000_000_00),     # $100M-$30B
            2: (20_000_000_00, 10_000_000_000_00),       # $20M-$10B
            3: (5_000_000_00, 5_000_000_000_00),         # $5M-$5B
            4: (500_000_00, 2_000_000_000_00),           # $500K-$2B
            5: (50_000_00, 1_000_000_000_00),            # $50K-$1B
        }
        low, high = expected.get(tier, (0, float('inf')))

        monthly_handle = monthly.groupby('period_end')['handle'].sum()
        median_monthly = monthly_handle.median()

        if pd.isna(median_monthly) or median_monthly == 0:
            return

        if median_monthly > high * 10:
            self._add(Level.SUSPECT, "unit_error",
                      f"Median monthly handle ${median_monthly/100:,.0f} is 10x+ above expected range",
                      f"Tier {tier} expects ${low/100:,.0f}-${high/100:,.0f}. "
                      "Values may be in cents when scraper already returns cents, or parsed thousands as dollars.")
        elif median_monthly < low / 10:
            self._add(Level.SUSPECT, "unit_error",
                      f"Median monthly handle ${median_monthly/100:,.0f} is 10x+ below expected range",
                      f"Tier {tier} expects ${low/100:,.0f}-${high/100:,.0f}. "
                      "Values may be in dollars when they should be in thousands, or data is partial.")

        # Check if gross_revenue or tax seem off relative to handle
        if 'tax_paid' in df.columns and df['tax_paid'].notna().any():
            mask = df['handle'].notna() & df['tax_paid'].notna() & (df['handle'] != 0)
            sub = df[mask]
            if not sub.empty:
                tax_ratio = (sub['tax_paid'].astype(float) / sub['handle'].astype(float)).median()
                if tax_ratio > 0.5:
                    self._add(Level.SUSPECT, "unit_error",
                              f"Median tax/handle ratio is {tax_ratio:.1%} (expected <20%)",
                              "Tax_paid may be in different units than handle")
                elif tax_ratio > 0 and tax_ratio < 0.0001:
                    self._add(Level.REVIEW, "unit_error",
                              f"Median tax/handle ratio is {tax_ratio:.4%} (unusually low)",
                              "Tax_paid may be in different units than handle")

    # ------------------------------------------------------------------
    # 3. YTD-as-monthly detection
    # ------------------------------------------------------------------
    def _check_ytd_pattern(self, df: pd.DataFrame):
        """Detect year-to-date accumulation being parsed as monthly data."""
        if 'handle' not in df.columns:
            return
        monthly = df[df['period_type'] == 'monthly'].copy()
        if len(monthly) < 4:
            return

        # Group by operator AND channel to avoid false positives
        # (e.g., AZ retail $389K followed by online $99M for same operator)
        # Use operator_reported for venue-level granularity
        op_col = 'operator_reported' if 'operator_reported' in monthly.columns else 'operator_standard'
        group_cols = [op_col]
        if 'channel' in monthly.columns:
            group_cols.append('channel')

        groups = monthly.groupby(group_cols)

        ytd_suspects = []
        for group_key, op_data in groups:
            op = group_key[0] if isinstance(group_key, tuple) else group_key
            if op in ('TOTAL', 'ALL'):
                continue

            op_data = op_data.sort_values('period_end')
            if len(op_data) < 4:
                continue

            handles = op_data['handle'].dropna().values
            if len(handles) < 4:
                continue

            label = f"{op}" if len(group_cols) == 1 else f"{op} ({group_key[1]})"

            # YTD pattern: values increase monotonically within each fiscal year
            # AND the last month is much larger than the first
            # AND there's a big drop at year boundaries (Jan or Jul)

            # Check for a sudden huge jump (>7x) in the last entry
            if len(handles) >= 2 and handles[-2] > 0:
                ratio = handles[-1] / handles[-2]
                if ratio > 7:
                    ytd_suspects.append(
                        f"{label}: last month {ratio:.1f}x previous "
                        f"(${handles[-1]/100:,.0f} vs ${handles[-2]/100:,.0f})"
                    )
                    continue

            # Check monotonic increase pattern within calendar year segments
            pe = pd.to_datetime(op_data['period_end'])
            for year in pe.dt.year.unique():
                year_handles = op_data[pe.dt.year == year]['handle'].dropna().values
                if len(year_handles) < 5:
                    continue
                diffs = np.diff(year_handles)
                mono_pct = (diffs > 0).sum() / len(diffs)
                if mono_pct > 0.85:
                    # Also check if last value is much larger than first
                    # Require 10x+ growth AND last value > $10M to distinguish
                    # from natural operator ramp-up (launching from near $0)
                    growth = year_handles[-1] / year_handles[0] if year_handles[0] > 0 else 0
                    if growth > 10 and year_handles[-1] > 5_000_000_000:  # >$50M in cents
                        ytd_suspects.append(
                            f"{label} in {year}: monotonic increase ({mono_pct:.0%} of months up), "
                            f"last/first = {growth:.1f}x — likely YTD accumulation"
                        )

        if ytd_suspects:
            self._add(Level.SUSPECT, "ytd_as_monthly",
                      f"{len(ytd_suspects)} operator(s) show YTD accumulation pattern",
                      "\n".join(ytd_suspects[:5]))

    # ------------------------------------------------------------------
    # 4. Month-over-month spikes
    # ------------------------------------------------------------------
    def _check_month_over_month_spikes(self, df: pd.DataFrame):
        """Flag months where state-wide handle jumps or drops by >3x."""
        if 'handle' not in df.columns:
            return
        monthly = df[df['period_type'] == 'monthly']
        if monthly.empty:
            return

        # Aggregate handle per month (across operators/channels)
        state_monthly = (
            monthly.groupby('period_end')['handle']
            .sum()
            .sort_index()
            .dropna()
        )
        if len(state_monthly) < 3:
            return

        spikes = []
        vals = state_monthly.values
        dates = state_monthly.index

        for i in range(1, len(vals)):
            if vals[i-1] == 0:
                continue
            ratio = vals[i] / vals[i-1]
            if ratio > 5 or (ratio < 0.2 and ratio > 0):
                direction = "spike" if ratio > 1 else "drop"
                spikes.append(
                    f"{dates[i]}: {ratio:.1f}x {direction} "
                    f"(${vals[i]/100:,.0f} vs prior ${vals[i-1]/100:,.0f})"
                )

        if spikes:
            # Only SUSPECT if >2 spikes AND some are recent (within last 2 years)
            recent_cutoff = pd.Timestamp.now() - pd.DateOffset(years=2)
            recent_spikes = [s for s in spikes if pd.Timestamp(s.split(":")[0]) > recent_cutoff]
            level = Level.SUSPECT if len(recent_spikes) > 2 else Level.REVIEW
            self._add(level, "mom_spike",
                      f"{len(spikes)} month(s) with >5x handle change",
                      "\n".join(spikes[:5]))

    # ------------------------------------------------------------------
    # 5. Stale / repeated data
    # ------------------------------------------------------------------
    def _check_stale_data(self, df: pd.DataFrame):
        """Detect identical values across consecutive months (copy-paste bug)."""
        if 'handle' not in df.columns:
            return
        monthly = df[df['period_type'] == 'monthly']
        if monthly.empty:
            return

        # Use operator_reported for stale data checks (venue-level granularity)
        op_col = 'operator_reported' if 'operator_reported' in monthly.columns else 'operator_standard'
        operators = monthly[op_col].unique() if op_col in monthly.columns else ['ALL']
        stale_ops = []

        for op in operators:
            if op in ('TOTAL',):
                continue
            op_data = monthly[monthly[op_col] == op].sort_values('period_end')
            if len(op_data) < 3:
                continue

            handles = op_data['handle'].dropna().values
            if len(handles) < 3:
                continue

            # Count consecutive identical values
            consecutive = 0
            max_run = 0
            for i in range(1, len(handles)):
                if handles[i] == handles[i-1] and handles[i] != 0:
                    consecutive += 1
                    max_run = max(max_run, consecutive)
                else:
                    consecutive = 0

            if max_run >= 3:
                stale_ops.append(f"{op}: {max_run+1} consecutive identical handle values")

        if stale_ops:
            self._add(Level.SUSPECT, "stale_data",
                      f"{len(stale_ops)} operator(s) with repeated identical values across months",
                      "\n".join(stale_ops[:5]))

    # ------------------------------------------------------------------
    # 6. Double counting
    # ------------------------------------------------------------------
    def _check_double_counting(self, df: pd.DataFrame):
        """Check for TOTAL rows coexisting with operator rows, or combined + split channels."""
        # Operator double counting
        if 'operator_standard' in df.columns:
            has_total = (df['operator_standard'] == 'TOTAL').any()
            has_ops = (~df['operator_standard'].isin(['TOTAL', 'ALL', 'UNKNOWN'])).any()
            if has_total and has_ops:
                total_count = (df['operator_standard'] == 'TOTAL').sum()
                self._add(Level.REVIEW, "double_count",
                          f"Data has both TOTAL rows ({total_count}) and operator-level rows",
                          "Ensure downstream aggregations exclude TOTAL rows to avoid double counting")

        # Channel double counting
        if 'channel' in df.columns:
            channels = set(df['channel'].unique())
            has_combined = 'combined' in channels
            has_split = 'online' in channels or 'retail' in channels
            if has_combined and has_split:
                combined_count = (df['channel'] == 'combined').sum()
                split_count = df['channel'].isin(['online', 'retail']).sum()
                self._add(Level.REVIEW, "double_count",
                          f"Data has both 'combined' ({combined_count}) and split channels ({split_count})",
                          "Ensure downstream aggregations use one or the other, not both")

    # ------------------------------------------------------------------
    # 7. Cross-field sanity
    # ------------------------------------------------------------------
    def _check_cross_field_sanity(self, df: pd.DataFrame):
        """Verify logical relationships between financial fields."""
        checks_run = 0

        # promo_credits > gross_revenue (can't deduct more than you earned)
        if 'promo_credits' in df.columns and 'gross_revenue' in df.columns:
            mask = (
                df['promo_credits'].notna() & df['gross_revenue'].notna() &
                (df['gross_revenue'] > 0) & (df['promo_credits'] > 0)
            )
            sub = df[mask]
            if not sub.empty:
                checks_run += 1
                bad = sub['promo_credits'].astype(float) > sub['gross_revenue'].astype(float) * 1.5
                if bad.any():
                    pct = bad.sum() / len(sub)
                    self._add(Level.REVIEW, "cross_field",
                              f"promo_credits > 1.5x gross_revenue in {bad.sum()} rows ({pct:.0%})",
                              "Promo credits normally shouldn't exceed gross revenue")

        # tax_paid > net_revenue (tax rate > 100%)
        if 'tax_paid' in df.columns and 'net_revenue' in df.columns:
            mask = (
                df['tax_paid'].notna() & df['net_revenue'].notna() &
                (df['net_revenue'] > 0) & (df['tax_paid'] > 0)
            )
            sub = df[mask]
            if not sub.empty:
                checks_run += 1
                bad = sub['tax_paid'].astype(float) > sub['net_revenue'].astype(float)
                if bad.any():
                    pct = bad.sum() / len(sub)
                    if pct > 0.3:
                        self._add(Level.SUSPECT, "cross_field",
                                  f"tax_paid > net_revenue in {bad.sum()} rows ({pct:.0%})",
                                  "Tax shouldn't exceed revenue. Check if columns are misassigned "
                                  "or if tax is on gross not net.")
                    else:
                        self._add(Level.REVIEW, "cross_field",
                                  f"tax_paid > net_revenue in {bad.sum()} rows ({pct:.0%})")

        # net_revenue > gross_revenue (net should be <= gross)
        if 'net_revenue' in df.columns and 'gross_revenue' in df.columns:
            mask = (
                df['net_revenue'].notna() & df['gross_revenue'].notna() &
                (df['gross_revenue'] > 0)
            )
            sub = df[mask]
            if not sub.empty:
                checks_run += 1
                bad = sub['net_revenue'].astype(float) > sub['gross_revenue'].astype(float) * 1.01
                if bad.any():
                    pct = bad.sum() / len(sub)
                    if pct > 0.5:
                        self._add(Level.SUSPECT, "cross_field",
                                  f"net_revenue > gross_revenue in {bad.sum()} rows ({pct:.0%})",
                                  "Net revenue should be gross minus deductions. Columns may be swapped.")

        if checks_run == 0:
            self._add(Level.OK, "cross_field", "Not enough overlapping fields to cross-check")

    # ------------------------------------------------------------------
    # 8. Operator count stability
    # ------------------------------------------------------------------
    def _check_operator_count_stability(self, df: pd.DataFrame):
        """Flag sudden drops in operator count (suggests incomplete parse)."""
        # Use operator_reported (venue-level) for stability, fall back to operator_standard
        op_col = 'operator_reported' if 'operator_reported' in df.columns else 'operator_standard'
        if op_col not in df.columns:
            return
        monthly = df[(df['period_type'] == 'monthly') & ~df[op_col].isin(['TOTAL', 'ALL'])]
        if monthly.empty:
            return

        counts = monthly.groupby('period_end')[op_col].nunique().sort_index()
        if len(counts) < 3:
            return

        # Establish a baseline (median operator count)
        median_count = counts.median()
        if median_count < 2:
            return

        drops = []
        for period, count in counts.items():
            if count < median_count * 0.4:
                drops.append(f"{period}: {count} operators (median is {median_count:.0f})")

        if drops:
            self._add(Level.REVIEW, "operator_stability",
                      f"{len(drops)} month(s) with abnormally few operators",
                      "\n".join(drops[:5]))

    # ------------------------------------------------------------------
    # 9. Hold% stability
    # ------------------------------------------------------------------
    def _check_hold_stability(self, df: pd.DataFrame):
        """State-wide hold% should be relatively stable (5-12%). Large shifts signal unit errors."""
        if 'handle' not in df.columns or 'gross_revenue' not in df.columns:
            return
        monthly = df[df['period_type'] == 'monthly']
        if monthly.empty:
            return

        # Compute state-wide hold% per month
        agg = monthly.groupby('period_end').agg(
            total_handle=('handle', 'sum'),
            total_gr=('gross_revenue', 'sum'),
        )
        mask = agg['total_handle'].notna() & (agg['total_handle'] != 0) & agg['total_gr'].notna()
        agg = agg[mask]
        if len(agg) < 3:
            return

        agg['hold'] = agg['total_gr'].astype(float) / agg['total_handle'].astype(float)

        median_hold = agg['hold'].median()
        # Outliers: hold% more than 3x or less than 1/3 of median
        if median_hold <= 0:
            return

        outliers = agg[(agg['hold'] > median_hold * 3) | (agg['hold'] < median_hold / 3)]
        if not outliers.empty:
            details = []
            for period, row in outliers.iterrows():
                details.append(
                    f"{period}: hold={row['hold']:.2%} "
                    f"(handle=${row['total_handle']/100:,.0f}, gr=${row['total_gr']/100:,.0f})"
                )
            self._add(Level.REVIEW, "hold_stability",
                      f"{len(outliers)} month(s) with hold% far from median {median_hold:.2%}",
                      "\n".join(details[:5]))


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def format_finding(f: Finding) -> str:
    icon = {"SUSPECT": "X", "REVIEW": "?", "OK": "."}[f.level]
    line = f"    {icon} [{f.check}] {f.message}"
    if f.details:
        for d in f.details.split("\n"):
            line += f"\n        {d}"
    return line


def format_result(result: SenseCheckResult) -> str:
    name = STATE_REGISTRY.get(result.state_code, {}).get('name', result.state_code)
    status = "CLEAN" if result.clean else f"{len(result.suspects)} SUSPECT"
    lines = [f"\n{'='*70}"]
    lines.append(f"  {result.state_code} ({name})  —  {status}")
    lines.append(f"{'='*70}")

    for level_name, group_label in [(Level.SUSPECT, "SUSPECTS"), (Level.REVIEW, "REVIEW"), (Level.OK, "OK")]:
        group = [f for f in result.findings if f.level == level_name]
        if group:
            lines.append(f"\n  {group_label}:")
            for f in group:
                lines.append(format_finding(f))

    return "\n".join(lines)


def format_summary(results: list[SenseCheckResult]) -> str:
    clean = [r for r in results if r.clean]
    dirty = [r for r in results if not r.clean]

    lines = [f"\n{'='*70}"]
    lines.append(f"  SENSE CHECK SUMMARY")
    lines.append(f"{'='*70}")
    lines.append(f"  Total: {len(results)}  |  Clean: {len(clean)}  |  Issues: {len(dirty)}")

    lines.append(f"\n  {'State':<6} {'Suspects':>9} {'Reviews':>9} {'Status':>10}")
    lines.append(f"  {'─'*6} {'─'*9} {'─'*9} {'─'*10}")
    for r in sorted(results, key=lambda x: (-len(x.suspects), x.state_code)):
        status = "CLEAN" if r.clean else "SUSPECT"
        lines.append(
            f"  {r.state_code:<6} {len(r.suspects):>9} {len(r.reviews):>9} {status:>10}"
        )

    if dirty:
        lines.append(f"\n  STATES WITH SUSPECTED BUGS:")
        for r in sorted(dirty, key=lambda x: x.state_code):
            checks = [f.check for f in r.suspects]
            lines.append(f"    {r.state_code}: {', '.join(checks)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def sense_check_state(state_code: str) -> SenseCheckResult:
    csv_path = Path(f"data/processed/{state_code}.csv")
    if not csv_path.exists():
        checker = SenseChecker(state_code)
        checker._add(Level.SUSPECT, "file", f"No processed CSV at {csv_path}")
        return SenseCheckResult(state_code, checker.findings)

    df = pd.read_csv(csv_path)
    checker = SenseChecker(state_code)
    return checker.run(df)


def sense_check_all(states: list[str] | None = None) -> list[SenseCheckResult]:
    if states is None:
        processed = Path("data/processed")
        states = sorted(
            p.stem for p in processed.glob("*.csv")
            if p.stem != "all_states" and p.stem in STATE_REGISTRY
        )

    results = []
    for state_code in states:
        result = sense_check_state(state_code)
        results.append(result)
        print(format_result(result))

    print(format_summary(results))
    return results


def main():
    parser = argparse.ArgumentParser(description="Sense-check scraped state data for bugs")
    parser.add_argument("states", nargs="*", help="State codes to check (default: all)")
    args = parser.parse_args()

    states = [s.upper() for s in args.states] if args.states else None
    sense_check_all(states)


if __name__ == "__main__":
    main()

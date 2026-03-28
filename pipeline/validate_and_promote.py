"""
Data validation and promotion pipeline.
Validates scraped state data, then promotes to dashboard if it passes.

Usage:
    python -m pipeline.validate_and_promote              # validate all states
    python -m pipeline.validate_and_promote NY PA CT     # validate specific states
    python -m pipeline.validate_and_promote --promote    # validate + promote passing states
    python -m pipeline.validate_and_promote --fix        # auto-fix missing standard_ggr
"""

import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.config import STATE_REGISTRY
from scrapers.base_scraper import STANDARD_COLUMNS, MONEY_COLUMNS


# ---------------------------------------------------------------------------
# Severity levels
# ---------------------------------------------------------------------------
class Severity:
    ERROR = "ERROR"      # Blocks promotion
    WARNING = "WARNING"  # Logged but does not block
    INFO = "INFO"        # Informational


@dataclass
class Issue:
    severity: str
    check: str
    message: str
    rows_affected: int = 0


@dataclass
class ValidationResult:
    state_code: str
    passed: bool
    issues: list[Issue] = field(default_factory=list)
    row_count: int = 0
    date_range: str = ""
    channels: dict = field(default_factory=dict)
    fields_populated: list[str] = field(default_factory=list)
    fields_missing: list[str] = field(default_factory=list)

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------
class DataValidator:
    def __init__(self, state_code: str):
        self.state_code = state_code.upper()
        if self.state_code not in STATE_REGISTRY:
            raise ValueError(f"Unknown state code: {self.state_code}")
        self.config = STATE_REGISTRY[self.state_code]
        self.issues: list[Issue] = []

    def _add(self, severity: str, check: str, msg: str, rows: int = 0):
        self.issues.append(Issue(severity, check, msg, rows))

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """Run all validation checks and return a result."""
        self.issues = []

        self._check_schema(df)
        self._check_empty(df)
        if df.empty:
            return self._build_result(df, passed=False)

        self._check_types(df)
        self._check_duplicates(df)
        self._check_date_range(df)
        self._check_date_gaps(df)
        self._check_channels(df)
        self._check_negative_handle(df)
        self._check_hold_sanity(df)
        self._check_ggr_consistency(df)
        self._check_net_revenue_consistency(df)
        self._check_tax_consistency(df)
        self._check_handle_magnitude(df)
        self._check_operator_normalization(df)
        self._check_standard_ggr(df)
        self._check_field_completeness(df)

        has_errors = any(i.severity == Severity.ERROR for i in self.issues)
        return self._build_result(df, passed=not has_errors)

    def _build_result(self, df: pd.DataFrame, passed: bool) -> ValidationResult:
        channels = df['channel'].value_counts().to_dict() if 'channel' in df.columns and not df.empty else {}
        money_present = [c for c in MONEY_COLUMNS if c in df.columns and df[c].notna().any()]
        money_missing = [c for c in MONEY_COLUMNS if c not in df.columns or not df[c].notna().any()]

        date_range = ""
        if not df.empty and 'period_end' in df.columns:
            date_range = f"{df['period_end'].min()} to {df['period_end'].max()}"

        return ValidationResult(
            state_code=self.state_code,
            passed=passed,
            issues=self.issues,
            row_count=len(df),
            date_range=date_range,
            channels=channels,
            fields_populated=money_present,
            fields_missing=money_missing,
        )

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_schema(self, df: pd.DataFrame):
        """Verify required columns exist."""
        required = {'state_code', 'period_end', 'period_type', 'operator_reported', 'operator_standard', 'channel'}
        missing = required - set(df.columns)
        if missing:
            self._add(Severity.ERROR, "schema", f"Missing required columns: {missing}")

    def _check_empty(self, df: pd.DataFrame):
        """Check for empty dataset."""
        if df.empty:
            self._add(Severity.ERROR, "empty", "No data rows")

    def _check_types(self, df: pd.DataFrame):
        """Verify money columns are numeric."""
        for col in MONEY_COLUMNS:
            if col in df.columns and df[col].notna().any():
                non_numeric = pd.to_numeric(df[col], errors='coerce').isna() & df[col].notna()
                if non_numeric.any():
                    self._add(Severity.ERROR, "types",
                              f"{col}: {non_numeric.sum()} non-numeric values", non_numeric.sum())

    def _check_duplicates(self, df: pd.DataFrame):
        """Check for duplicate rows on the composite key."""
        key_cols = ['state_code', 'period_end', 'period_type', 'operator_reported', 'channel', 'sport_category']
        existing = [c for c in key_cols if c in df.columns]
        dupes = df.duplicated(subset=existing, keep=False)
        if dupes.any():
            n = dupes.sum()
            self._add(Severity.ERROR, "duplicates",
                      f"{n} duplicate rows on key ({', '.join(existing)})", n)

    def _check_date_range(self, df: pd.DataFrame):
        """Flag future dates or dates before state launch."""
        if 'period_end' not in df.columns:
            return
        pe = pd.to_datetime(df['period_end'])
        today = pd.Timestamp.now()

        future = pe > today
        if future.any():
            self._add(Severity.ERROR, "date_range",
                      f"{future.sum()} rows with future dates", future.sum())

        launch = self.config.get('launch_date')
        if launch:
            launch_dt = pd.Timestamp(launch)
            pre_launch = pe < launch_dt
            if pre_launch.any():
                self._add(Severity.WARNING, "date_range",
                          f"{pre_launch.sum()} rows before launch date {launch}", pre_launch.sum())

    def _check_date_gaps(self, df: pd.DataFrame):
        """Detect gaps in monthly reporting."""
        if 'period_end' not in df.columns:
            return
        monthly = df[df['period_type'] == 'monthly'].copy()
        if monthly.empty:
            return

        pe = pd.to_datetime(monthly['period_end'])
        months = sorted(pe.dt.to_period('M').unique())
        if len(months) < 2:
            return

        gaps = []
        for i in range(1, len(months)):
            expected = months[i - 1] + 1
            if months[i] != expected:
                gap_start = expected
                gap_end = months[i] - 1
                gaps.append(f"{gap_start}-{gap_end}" if gap_start != gap_end else str(gap_start))

        if gaps:
            self._add(Severity.WARNING, "date_gaps",
                      f"Missing months: {', '.join(gaps[:10])}" +
                      (f" (+{len(gaps)-10} more)" if len(gaps) > 10 else ""))

    def _check_channels(self, df: pd.DataFrame):
        """Validate channel values and flag missing retail/online split."""
        if 'channel' not in df.columns:
            return
        valid = {'online', 'retail', 'combined'}
        actual = set(df['channel'].dropna().unique())
        bad = actual - valid
        if bad:
            self._add(Severity.WARNING, "channels", f"Non-standard channel values: {bad}")

        has_split = self.config.get('has_channel_split', False)
        has_online = 'online' in actual
        has_retail = 'retail' in actual

        if has_split and not has_online and not has_retail:
            self._add(Severity.WARNING, "channels",
                      "Config says channel split available but data only has 'combined'")

        # Report channel breakdown as info
        channel_counts = df['channel'].value_counts().to_dict()
        self._add(Severity.INFO, "channels", f"Channel breakdown: {channel_counts}")

    def _check_negative_handle(self, df: pd.DataFrame):
        """Flag negative handle values. Small counts are normal adjustments (reversals, wind-downs)."""
        if 'handle' not in df.columns:
            return
        neg = df['handle'].notna() & (df['handle'] < 0)
        if neg.any():
            n = neg.sum()
            pct = n / len(df) * 100
            # A few negative rows are normal (adjustments, operator wind-downs, COVID)
            # Only ERROR if > 5% of rows are negative
            sev = Severity.ERROR if pct > 5 else Severity.WARNING
            self._add(sev, "negative_handle",
                      f"{n} rows with negative handle ({pct:.1f}% of data)", n)

    def _check_hold_sanity(self, df: pd.DataFrame):
        """Check hold percentage is within reasonable bounds."""
        if 'hold_pct' not in df.columns:
            return
        hp = df['hold_pct'].dropna()
        if hp.empty:
            return

        # Flag rows outside 1%-40%
        out_of_range = (hp < 0.01) | (hp > 0.40)
        if out_of_range.any():
            n = out_of_range.sum()
            examples = hp[out_of_range].head(3).tolist()
            self._add(Severity.WARNING, "hold_sanity",
                      f"{n} rows with hold% outside 1-40% range (e.g. {[f'{x:.2%}' for x in examples]})", n)

        # Flag negative hold
        neg_hold = hp < 0
        if neg_hold.any():
            self._add(Severity.WARNING, "hold_sanity",
                      f"{neg_hold.sum()} rows with negative hold%", neg_hold.sum())

    def _check_ggr_consistency(self, df: pd.DataFrame):
        """Check: handle - payouts ≈ gross_revenue (within 2%)."""
        needed = ['handle', 'payouts', 'gross_revenue']
        if not all(c in df.columns for c in needed):
            return
        mask = df['handle'].notna() & df['payouts'].notna() & df['gross_revenue'].notna()
        subset = df[mask]
        if subset.empty:
            return

        computed = subset['handle'].astype(float) - subset['payouts'].astype(float)
        reported = subset['gross_revenue'].astype(float)

        # Avoid division by zero
        denom = reported.abs().clip(lower=1)
        pct_diff = ((computed - reported) / denom).abs()
        bad = pct_diff > 0.02
        if bad.any():
            n = bad.sum()
            max_diff = pct_diff[bad].max()
            self._add(Severity.WARNING, "ggr_consistency",
                      f"{n} rows where handle-payouts != gross_revenue (>2% diff, max {max_diff:.1%})", n)

    def _check_net_revenue_consistency(self, df: pd.DataFrame):
        """Check: gross_revenue - promo_credits ≈ net_revenue (within 1%)."""
        needed = ['gross_revenue', 'promo_credits', 'net_revenue']
        if not all(c in df.columns for c in needed):
            return
        mask = df['gross_revenue'].notna() & df['promo_credits'].notna() & df['net_revenue'].notna()
        subset = df[mask]
        if subset.empty:
            return

        computed = subset['gross_revenue'].astype(float) - subset['promo_credits'].astype(float)
        reported = subset['net_revenue'].astype(float)

        denom = reported.abs().clip(lower=1)
        pct_diff = ((computed - reported) / denom).abs()
        bad = pct_diff > 0.01
        if bad.any():
            n = bad.sum()
            max_diff = pct_diff[bad].max()
            self._add(Severity.WARNING, "net_revenue_consistency",
                      f"{n} rows where gross-promos != net_revenue (>1% diff, max {max_diff:.1%})", n)

    def _check_tax_consistency(self, df: pd.DataFrame):
        """Check tax_paid against expected tax rate (within 10% tolerance)."""
        if 'tax_paid' not in df.columns or not df['tax_paid'].notna().any():
            return

        # Determine which tax basis to check against
        online_rate = self.config.get('online_tax_rate')
        retail_rate = self.config.get('retail_tax_rate')
        tax_basis = self.config.get('tax_basis', '')

        if online_rate is None and retail_rate is None:
            return  # Revenue share or graduated — can't check

        # For TN, tax is on handle not revenue
        if tax_basis == 'handle':
            self._check_tax_on_handle(df, online_rate, retail_rate)
            return

        # Check by channel
        for channel_val, rate in [('online', online_rate), ('retail', retail_rate)]:
            if rate is None:
                continue
            channel_df = df[df['channel'] == channel_val] if 'channel' in df.columns else df
            if channel_df.empty:
                continue

            # Try net_revenue first (most common tax basis), then gross_revenue
            rev_col = None
            if 'net_revenue' in channel_df.columns and channel_df['net_revenue'].notna().any():
                rev_col = 'net_revenue'
            elif 'gross_revenue' in channel_df.columns and channel_df['gross_revenue'].notna().any():
                rev_col = 'gross_revenue'

            if rev_col is None:
                continue

            mask = channel_df[rev_col].notna() & channel_df['tax_paid'].notna() & (channel_df[rev_col] != 0)
            subset = channel_df[mask]
            if subset.empty:
                continue

            expected = subset[rev_col].astype(float) * rate
            actual = subset['tax_paid'].astype(float)
            denom = expected.abs().clip(lower=1)
            pct_diff = ((actual - expected) / denom).abs()
            bad = pct_diff > 0.10
            if bad.any():
                median_diff = pct_diff[bad].median()
                self._add(Severity.WARNING, "tax_consistency",
                          f"{channel_val}: {bad.sum()} rows where tax_paid differs from "
                          f"{rev_col}×{rate:.1%} by >10% (median diff {median_diff:.1%})",
                          bad.sum())

    def _check_tax_on_handle(self, df, online_rate, retail_rate):
        """Special check for TN-style handle-based tax."""
        if 'handle' not in df.columns:
            return
        rate = online_rate or retail_rate
        if rate is None:
            return
        mask = df['handle'].notna() & df['tax_paid'].notna() & (df['handle'] != 0)
        subset = df[mask]
        if subset.empty:
            return
        expected = subset['handle'].astype(float) * rate
        actual = subset['tax_paid'].astype(float)
        denom = expected.abs().clip(lower=1)
        pct_diff = ((actual - expected) / denom).abs()
        bad = pct_diff > 0.10
        if bad.any():
            self._add(Severity.WARNING, "tax_consistency",
                      f"{bad.sum()} rows where tax_paid differs from handle×{rate} by >10%",
                      bad.sum())

    def _check_handle_magnitude(self, df: pd.DataFrame):
        """Sanity check: handle magnitudes reasonable for the state's tier."""
        if 'handle' not in df.columns or not df['handle'].notna().any():
            return

        monthly = df[df['period_type'] == 'monthly']
        if monthly.empty:
            return

        # Sum handle per month across all operators/channels
        monthly_totals = monthly.groupby('period_end')['handle'].sum()
        avg_monthly = monthly_totals.mean()

        if pd.isna(avg_monthly) or avg_monthly == 0:
            return

        tier = self.config.get('tier', 99)

        # Values are in cents
        thresholds = {
            1: (50_000_000_00, 100_000_000_000_00),    # $50M - $100B
            2: (10_000_000_00, 50_000_000_000_00),     # $10M - $50B
            3: (1_000_000_00, 20_000_000_000_00),      # $1M - $20B
            4: (100_000_00, 10_000_000_000_00),        # $100K - $10B
            5: (10_000_00, 5_000_000_000_00),          # $10K - $5B
        }
        low, high = thresholds.get(tier, (0, float('inf')))

        if avg_monthly < low:
            self._add(Severity.WARNING, "handle_magnitude",
                      f"Avg monthly handle ${avg_monthly/100:,.0f} seems low for Tier {tier}")
        elif avg_monthly > high:
            self._add(Severity.ERROR, "handle_magnitude",
                      f"Avg monthly handle ${avg_monthly/100:,.0f} seems impossibly high for Tier {tier}")

    def _check_operator_normalization(self, df: pd.DataFrame):
        """Flag operators that weren't normalized (raw == reported)."""
        if 'operator_raw' not in df.columns or 'operator_reported' not in df.columns:
            return
        unresolved = df[
            (df['operator_reported'] == df['operator_raw']) &
            ~df['operator_reported'].isin(['ALL', 'TOTAL', 'UNKNOWN'])
        ]
        if not unresolved.empty:
            ops = unresolved['operator_reported'].unique().tolist()
            if len(ops) > 5:
                display = ops[:5] + [f"...+{len(ops)-5} more"]
            else:
                display = ops
            self._add(Severity.WARNING, "operator_normalization",
                      f"{len(ops)} unmapped operator(s): {display}", len(unresolved))

    def _check_standard_ggr(self, df: pd.DataFrame):
        """Check if standard_ggr column is present and populated."""
        if 'standard_ggr' not in df.columns:
            self._add(Severity.WARNING, "standard_ggr", "standard_ggr column missing from CSV")
            return
        null_count = df['standard_ggr'].isna().sum()
        if null_count == len(df):
            self._add(Severity.WARNING, "standard_ggr", "standard_ggr is all null")
        elif null_count > 0:
            self._add(Severity.INFO, "standard_ggr",
                      f"standard_ggr: {null_count}/{len(df)} null values")

    def _check_field_completeness(self, df: pd.DataFrame):
        """Report which money fields are populated vs missing."""
        populated = []
        missing = []
        for col in MONEY_COLUMNS:
            if col in df.columns and df[col].notna().any():
                pct = df[col].notna().sum() / len(df) * 100
                populated.append(f"{col}({pct:.0f}%)")
            else:
                missing.append(col)
        self._add(Severity.INFO, "completeness", f"Fields present: {', '.join(populated)}")
        if missing:
            self._add(Severity.INFO, "completeness", f"Fields absent: {', '.join(missing)}")


# ---------------------------------------------------------------------------
# Fix: backfill standard_ggr
# ---------------------------------------------------------------------------
def fix_standard_ggr(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Ensure standard_ggr column exists and is computed where possible.

    Tiered approach:
      1. handle - payouts (exact definition)
      2. gross_revenue as fallback IF hold% is sane (1-30%)
      3. Leave null otherwise

    Returns (fixed_df, stats_dict).
    """
    df = df.copy()
    stats = {"tier1": 0, "tier2": 0, "skipped": 0, "already_set": 0}

    if 'standard_ggr' not in df.columns:
        df['standard_ggr'] = pd.NA

    already = df['standard_ggr'].notna()
    stats["already_set"] = int(already.sum())
    needs = ~already

    # Tier 1: handle - payouts
    if needs.any() and 'handle' in df.columns and 'payouts' in df.columns:
        can = needs & df['handle'].notna() & df['payouts'].notna()
        if can.any():
            df.loc[can, 'standard_ggr'] = (
                df.loc[can, 'handle'].astype(float) - df.loc[can, 'payouts'].astype(float)
            ).round(0).astype('Int64')
            stats["tier1"] = int(can.sum())
            needs = df['standard_ggr'].isna()

    # Tier 2: use gross_revenue where hold% is sane (1-30%)
    if needs.any() and 'handle' in df.columns and 'gross_revenue' in df.columns:
        can = (
            needs &
            df['handle'].notna() & (df['handle'] != 0) &
            df['gross_revenue'].notna()
        )
        if can.any():
            hold = df.loc[can, 'gross_revenue'].astype(float) / df.loc[can, 'handle'].astype(float)
            sane = can.copy()
            sane.loc[can] = (hold > 0.01) & (hold < 0.30)
            if sane.any():
                df.loc[sane, 'standard_ggr'] = (
                    df.loc[sane, 'gross_revenue'].astype(float)
                ).round(0).astype('Int64')
                stats["tier2"] = int(sane.sum())

    stats["skipped"] = int(df['standard_ggr'].isna().sum())
    return df, stats


# ---------------------------------------------------------------------------
# Promote: copy validated data to dashboard
# ---------------------------------------------------------------------------
def promote_state(state_code: str, df: pd.DataFrame, fix: bool = False):
    """Promote validated data to dashboard/dist/data/."""
    fix_stats = None
    if fix:
        df, fix_stats = fix_standard_ggr(df)

    # Ensure column order matches STANDARD_COLUMNS
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[STANDARD_COLUMNS]

    # Save back to processed
    processed_path = Path(f"data/processed/{state_code}.csv")
    df.to_csv(processed_path, index=False)

    # Copy to dashboard
    dashboard_path = Path(f"dashboard/dist/data/{state_code}.csv")
    if dashboard_path.parent.exists():
        shutil.copy2(processed_path, dashboard_path)

    return processed_path, dashboard_path, fix_stats


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------
def format_result(result: ValidationResult) -> str:
    """Format a validation result for display."""
    status = "PASS" if result.passed else "FAIL"
    lines = [f"\n{'='*70}"]
    lines.append(f"  {result.state_code} ({STATE_REGISTRY[result.state_code]['name']})  —  {status}")
    lines.append(f"{'='*70}")
    lines.append(f"  Rows: {result.row_count}  |  Range: {result.date_range}")
    lines.append(f"  Channels: {result.channels}")

    errors = result.errors
    warnings = result.warnings
    infos = [i for i in result.issues if i.severity == Severity.INFO]

    if errors:
        lines.append(f"\n  ERRORS ({len(errors)}):")
        for e in errors:
            rows_str = f" [{e.rows_affected} rows]" if e.rows_affected else ""
            lines.append(f"    x {e.check}: {e.message}{rows_str}")

    if warnings:
        lines.append(f"\n  WARNINGS ({len(warnings)}):")
        for w in warnings:
            rows_str = f" [{w.rows_affected} rows]" if w.rows_affected else ""
            lines.append(f"    ! {w.check}: {w.message}{rows_str}")

    if infos:
        lines.append(f"\n  INFO:")
        for i in infos:
            lines.append(f"    - {i.check}: {i.message}")

    return "\n".join(lines)


def format_summary(results: list[ValidationResult]) -> str:
    """Format a summary table across all states."""
    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]

    lines = [f"\n{'='*70}"]
    lines.append(f"  VALIDATION SUMMARY")
    lines.append(f"{'='*70}")
    lines.append(f"  Total: {len(results)}  |  Passed: {len(passed)}  |  Failed: {len(failed)}")
    lines.append("")

    # Channel summary
    online_states = [r.state_code for r in results if 'online' in r.channels]
    retail_states = [r.state_code for r in results if 'retail' in r.channels]
    combined_only = [r.state_code for r in results
                     if 'combined' in r.channels and 'online' not in r.channels and 'retail' not in r.channels]

    lines.append(f"  Online data:   {len(online_states)} states — {', '.join(online_states)}")
    lines.append(f"  Retail data:   {len(retail_states)} states — {', '.join(retail_states)}")
    lines.append(f"  Combined only: {len(combined_only)} states — {', '.join(combined_only)}")
    lines.append("")

    # Per-state row
    lines.append(f"  {'State':<6} {'Rows':>6} {'Ch':>12} {'Errors':>7} {'Warns':>7} {'Status':>8}")
    lines.append(f"  {'─'*6} {'─'*6} {'─'*12} {'─'*7} {'─'*7} {'─'*8}")
    for r in sorted(results, key=lambda x: x.state_code):
        ch_str = '/'.join(sorted(r.channels.keys()))[:12]
        status = "PASS" if r.passed else "FAIL"
        lines.append(
            f"  {r.state_code:<6} {r.row_count:>6} {ch_str:>12} "
            f"{len(r.errors):>7} {len(r.warnings):>7} {status:>8}"
        )

    if failed:
        lines.append(f"\n  FAILED STATES:")
        for r in sorted(failed, key=lambda x: x.state_code):
            error_msgs = [e.check for e in r.errors]
            lines.append(f"    {r.state_code}: {', '.join(error_msgs)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def validate_state(state_code: str) -> ValidationResult:
    """Load and validate a single state."""
    csv_path = Path(f"data/processed/{state_code}.csv")
    if not csv_path.exists():
        validator = DataValidator(state_code)
        validator._add(Severity.ERROR, "file", f"No processed CSV at {csv_path}")
        return ValidationResult(
            state_code=state_code, passed=False, issues=validator.issues
        )

    df = pd.read_csv(csv_path)
    validator = DataValidator(state_code)
    return validator.validate(df)


def validate_and_promote(states: list[str] | None = None,
                         promote: bool = False,
                         fix: bool = False) -> list[ValidationResult]:
    """Validate states, optionally promote passing ones."""
    if states is None:
        # All states that have processed CSVs
        processed = Path("data/processed")
        states = sorted(
            p.stem for p in processed.glob("*.csv")
            if p.stem != "all_states" and p.stem in STATE_REGISTRY
        )

    results = []
    promoted = []

    for state_code in states:
        result = validate_state(state_code)
        results.append(result)
        print(format_result(result))

        if promote and result.passed:
            csv_path = Path(f"data/processed/{state_code}.csv")
            df = pd.read_csv(csv_path)
            proc_path, dash_path, fix_stats = promote_state(state_code, df, fix=fix)
            promoted.append(state_code)
            stats_str = ""
            if fix_stats:
                parts = []
                if fix_stats["tier1"]: parts.append(f"tier1(h-p)={fix_stats['tier1']}")
                if fix_stats["tier2"]: parts.append(f"tier2(gr)={fix_stats['tier2']}")
                if fix_stats["skipped"]: parts.append(f"null={fix_stats['skipped']}")
                stats_str = f" [{', '.join(parts)}]" if parts else ""
            print(f"  >> Promoted {state_code} to {dash_path}{stats_str}")

        elif promote and fix and not result.passed:
            # Check if the only errors are fixable
            fixable_errors = all(
                e.check in ('standard_ggr',) for e in result.errors
            )
            if fixable_errors or not result.errors:
                csv_path = Path(f"data/processed/{state_code}.csv")
                if csv_path.exists():
                    df = pd.read_csv(csv_path)
                    proc_path, dash_path, fix_stats = promote_state(state_code, df, fix=True)
                    promoted.append(state_code)
                    print(f"  >> Fixed + Promoted {state_code} to {dash_path}")

    print(format_summary(results))

    if promoted:
        print(f"\n  Promoted {len(promoted)} states: {', '.join(promoted)}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Validate and promote state sports betting data")
    parser.add_argument("states", nargs="*", help="State codes to validate (default: all)")
    parser.add_argument("--promote", action="store_true", help="Promote passing states to dashboard")
    parser.add_argument("--fix", action="store_true", help="Auto-fix missing standard_ggr before promotion")
    args = parser.parse_args()

    states = [s.upper() for s in args.states] if args.states else None
    validate_and_promote(states=states, promote=args.promote, fix=args.fix)


if __name__ == "__main__":
    main()

"""
Derived metrics computation for sports betting data.
All metrics computed on DataFrames with money values in CENTS.
"""

import pandas as pd
import numpy as np


def compute_hold_pct(df: pd.DataFrame) -> pd.DataFrame:
    """Compute hold percentage (gross_revenue / handle)."""
    df = df.copy()
    mask = df['handle'].notna() & (df['handle'] != 0) & df['gross_revenue'].notna()
    df.loc[mask, 'hold_pct'] = df.loc[mask, 'gross_revenue'].astype(float) / df.loc[mask, 'handle'].astype(float)
    return df


def compute_net_hold_pct(df: pd.DataFrame) -> pd.DataFrame:
    """Compute net hold percentage (net_revenue / handle)."""
    df = df.copy()
    mask = df['handle'].notna() & (df['handle'] != 0) & df['net_revenue'].notna()
    df.loc[mask, 'net_hold_pct'] = df.loc[mask, 'net_revenue'].astype(float) / df.loc[mask, 'handle'].astype(float)
    return df


def compute_promo_intensity(df: pd.DataFrame) -> pd.DataFrame:
    """Compute promo intensity (promo_credits / gross_revenue)."""
    df = df.copy()
    mask = df['gross_revenue'].notna() & (df['gross_revenue'] != 0) & df['promo_credits'].notna()
    df.loc[mask, 'promo_intensity'] = df.loc[mask, 'promo_credits'].astype(float) / df.loc[mask, 'gross_revenue'].astype(float)
    return df


def compute_effective_tax_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Compute effective tax rate (tax_paid / net_revenue)."""
    df = df.copy()
    mask = df['net_revenue'].notna() & (df['net_revenue'] != 0) & df['tax_paid'].notna()
    df.loc[mask, 'effective_tax_rate'] = df.loc[mask, 'tax_paid'].astype(float) / df.loc[mask, 'net_revenue'].astype(float)
    return df


def compute_market_share(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute market share within each state+period.
    Excludes TOTAL rows. Adds handle_market_share and revenue_market_share columns.
    """
    df = df.copy()
    operator_df = df[~df['operator_standard'].isin(['TOTAL', 'ALL', 'UNKNOWN'])].copy()

    if operator_df.empty:
        df['handle_market_share'] = None
        df['revenue_market_share'] = None
        return df

    # Handle market share
    state_totals = operator_df.groupby(['state_code', 'period_end', 'period_type'])['handle'].sum().reset_index()
    state_totals.rename(columns={'handle': '_total_handle'}, inplace=True)
    operator_df = operator_df.merge(state_totals, on=['state_code', 'period_end', 'period_type'], how='left')
    mask = operator_df['_total_handle'].notna() & (operator_df['_total_handle'] != 0)
    operator_df.loc[mask, 'handle_market_share'] = operator_df.loc[mask, 'handle'].astype(float) / operator_df.loc[mask, '_total_handle'].astype(float)

    # Revenue market share
    if 'net_revenue' in operator_df.columns:
        rev_totals = operator_df.groupby(['state_code', 'period_end', 'period_type'])['net_revenue'].sum().reset_index()
        rev_totals.rename(columns={'net_revenue': '_total_revenue'}, inplace=True)
        operator_df = operator_df.merge(rev_totals, on=['state_code', 'period_end', 'period_type'], how='left')
        mask = operator_df['_total_revenue'].notna() & (operator_df['_total_revenue'] != 0)
        operator_df.loc[mask, 'revenue_market_share'] = operator_df.loc[mask, 'net_revenue'].astype(float) / operator_df.loc[mask, '_total_revenue'].astype(float)

    operator_df.drop(columns=['_total_handle', '_total_revenue'], errors='ignore', inplace=True)

    # Merge back
    df = df.merge(
        operator_df[['state_code', 'period_end', 'period_type', 'operator_standard', 'channel',
                      'handle_market_share', 'revenue_market_share']].drop_duplicates(),
        on=['state_code', 'period_end', 'period_type', 'operator_standard', 'channel'],
        how='left',
        suffixes=('', '_new'),
    )
    for col in ['handle_market_share', 'revenue_market_share']:
        if f'{col}_new' in df.columns:
            df[col] = df[f'{col}_new'].combine_first(df.get(col))
            df.drop(columns=[f'{col}_new'], inplace=True)

    return df


def compute_yoy_changes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute year-over-year changes for handle and revenue.
    Only works on monthly data.
    """
    df = df.copy()
    monthly = df[df['period_type'] == 'monthly'].copy()
    if monthly.empty:
        return df

    monthly['period_end'] = pd.to_datetime(monthly['period_end'])
    monthly['_month'] = monthly['period_end'].dt.month
    monthly['_year'] = monthly['period_end'].dt.year

    group_cols = ['state_code', 'operator_standard', 'channel', '_month']
    monthly = monthly.sort_values(['state_code', 'operator_standard', 'channel', 'period_end'])

    # YoY handle change
    for col_name, base_col in [('yoy_handle_change_pct', 'handle'), ('yoy_revenue_change_pct', 'net_revenue')]:
        if base_col in monthly.columns:
            prior = monthly.copy()
            prior['_year'] = prior['_year'] + 1
            prior.rename(columns={base_col: f'_prior_{base_col}'}, inplace=True)
            monthly = monthly.merge(
                prior[group_cols + ['_year', f'_prior_{base_col}']],
                on=group_cols + ['_year'],
                how='left',
            )
            mask = monthly[f'_prior_{base_col}'].notna() & (monthly[f'_prior_{base_col}'] != 0)
            monthly.loc[mask, col_name] = (
                (monthly.loc[mask, base_col].astype(float) - monthly.loc[mask, f'_prior_{base_col}'].astype(float))
                / monthly.loc[mask, f'_prior_{base_col}'].astype(float)
            )
            monthly.drop(columns=[f'_prior_{base_col}'], inplace=True)

    monthly.drop(columns=['_month', '_year'], errors='ignore', inplace=True)
    return monthly


def compute_all_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all derived metric computations."""
    df = compute_hold_pct(df)
    df = compute_net_hold_pct(df)
    df = compute_promo_intensity(df)
    df = compute_effective_tax_rate(df)
    df = compute_market_share(df)
    return df

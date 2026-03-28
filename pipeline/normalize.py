"""
Data normalization pipeline — combines state CSVs into unified dataset.
"""

import pandas as pd
from pathlib import Path
from scrapers.base_scraper import STANDARD_COLUMNS


def load_all_processed(processed_dir: str = "data/processed") -> pd.DataFrame:
    """Load all processed state CSVs into a single DataFrame."""
    processed = Path(processed_dir)
    all_dfs = []

    for csv_path in sorted(processed.glob("*.csv")):
        if csv_path.name == "all_states.csv":
            continue  # Skip the combined file
        try:
            df = pd.read_csv(csv_path)
            all_dfs.append(df)
        except Exception as e:
            print(f"Error loading {csv_path.name}: {e}")

    if not all_dfs:
        return pd.DataFrame(columns=STANDARD_COLUMNS)

    combined = pd.concat(all_dfs, ignore_index=True)

    # Ensure consistent types
    combined['period_start'] = pd.to_datetime(combined['period_start'])
    combined['period_end'] = pd.to_datetime(combined['period_end'])
    combined['scrape_timestamp'] = pd.to_datetime(combined['scrape_timestamp'])

    for col in ['handle', 'gross_revenue', 'promo_credits', 'net_revenue',
                'payouts', 'tax_paid', 'federal_excise_tax']:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors='coerce').astype('Int64')

    return combined


def normalize_and_export():
    """Load all state data, normalize, and export combined file."""
    df = load_all_processed()
    if df.empty:
        print("No processed data found")
        return df

    # Sort
    df = df.sort_values(['state_code', 'period_end', 'operator_standard', 'channel'])

    # Export combined
    output = Path("data/processed/all_states.csv")
    df.to_csv(output, index=False)
    print(f"Combined {len(df)} rows from {df['state_code'].nunique()} states into {output}")

    return df

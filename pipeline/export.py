"""
Export processed data to CSV, Excel, and JSON formats.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime


def export_csv(df: pd.DataFrame, output_path: str = "data/processed/all_states.csv"):
    """Export to CSV."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} rows to {output_path}")


def export_excel(df: pd.DataFrame, output_path: str = "data/processed/all_states.xlsx"):
    """Export to Excel with separate sheets per state."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        summary = df.groupby('state_code').agg(
            rows=('handle', 'count'),
            min_date=('period_end', 'min'),
            max_date=('period_end', 'max'),
            total_handle=('handle', 'sum'),
        ).reset_index()
        summary.to_excel(writer, sheet_name='Summary', index=False)

        # Per-state sheets
        for state in sorted(df['state_code'].unique()):
            state_df = df[df['state_code'] == state]
            state_df.to_excel(writer, sheet_name=state, index=False)

    print(f"Exported {len(df)} rows to {output_path}")


def export_json(df: pd.DataFrame, output_path: str = "data/processed/all_states.json"):
    """Export to JSON (records format)."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Convert timestamps to strings
    df = df.copy()
    for col in df.select_dtypes(include=['datetime64', 'datetimetz']).columns:
        df[col] = df[col].astype(str)

    records = df.to_dict(orient='records')
    with open(output_path, 'w') as f:
        json.dump(records, f, indent=2, default=str)

    print(f"Exported {len(records)} records to {output_path}")

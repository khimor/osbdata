"""
Database operations for Supabase Postgres.
Handles UPSERT logic keyed on (state_code, period_end, operator_standard, channel, sport_category).
"""

import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
TABLE_NAME = "monthly_data"

# Composite unique key for upsert
UPSERT_KEY_COLUMNS = [
    'state_code', 'period_end', 'period_type',
    'operator_standard', 'channel', 'sport_category',
]


def get_supabase_client():
    """Get a Supabase client instance."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set in .env file. "
            "Set them before running DB operations."
        )
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_dataframe(df: pd.DataFrame, table: str = TABLE_NAME, batch_size: int = 500) -> dict:
    """
    Upsert a DataFrame into the Supabase table.
    Returns dict with 'inserted', 'updated', 'errors' counts.
    """
    client = get_supabase_client()
    results = {'inserted': 0, 'updated': 0, 'errors': 0}

    # Convert DataFrame to list of dicts, handling NaN/NaT and numeric types
    records = df.to_dict(orient='records')

    for record in records:
        for key, val in list(record.items()):
            if val is None:
                continue
            # Handle pandas NA/NaN/NaT
            try:
                if pd.isna(val):
                    record[key] = None
                    continue
            except (TypeError, ValueError):
                pass
            # Convert datetime/date to string
            if isinstance(val, (datetime, pd.Timestamp)):
                record[key] = val.isoformat()
            elif hasattr(val, 'isoformat'):
                record[key] = val.isoformat()
            # Convert numpy/pandas int/float to Python native types
            elif hasattr(val, 'item'):  # numpy scalar
                record[key] = val.item()
            elif isinstance(val, float) and val == int(val):
                record[key] = int(val)

    # Batch upsert
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            response = client.table(table).upsert(
                batch,
                on_conflict=','.join(UPSERT_KEY_COLUMNS),
            ).execute()
            results['inserted'] += len(batch)
        except Exception as e:
            results['errors'] += len(batch)
            print(f"  DB error on batch {i//batch_size}: {e}")

    return results


def get_latest_period(state_code: str, table: str = TABLE_NAME):
    """Get the most recent period_end date for a state from the DB."""
    try:
        client = get_supabase_client()
        response = (
            client.table(table)
            .select("period_end")
            .eq("state_code", state_code)
            .order("period_end", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            return pd.to_datetime(response.data[0]['period_end']).date()
    except Exception:
        pass
    return None


def query_state_data(state_code: str, table: str = TABLE_NAME) -> pd.DataFrame:
    """Query all data for a state from the DB."""
    client = get_supabase_client()
    response = (
        client.table(table)
        .select("*")
        .eq("state_code", state_code)
        .order("period_end", desc=True)
        .execute()
    )
    if response.data:
        return pd.DataFrame(response.data)
    return pd.DataFrame()

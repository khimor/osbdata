"""
Connecticut Sports Wagering Scraper
Source: data.ct.gov (Socrata open data API)
Online dataset: xf6g-659c
Retail dataset: yb54-t38r
Format: JSON API (monthly data)
Launch: October 2021
Tax: 13.75% on GGR (after promo deductions)
"""

import sys
import json
from pathlib import Path
from datetime import date, datetime

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, fetch_with_retry

# Socrata dataset IDs on data.ct.gov
CT_ONLINE_DATASET = "xf6g-659c"
CT_RETAIL_DATASET = "yb54-t38r"
CT_API_BASE = "https://data.ct.gov/resource"

# Field mappings: Socrata API field → our field name
ONLINE_FIELDS = {
    "wagers": "handle",
    "patron_winnings": "payouts",
    "unadjusted_monthly_gaming": "gross_revenue",
    "promotional_deduction_5": "promo_credits",
    "total_gross_gaming": "net_revenue",
    "tax_payment_6": "tax_paid",
    "federal_excise_tax_3": "federal_excise_tax",
}

RETAIL_FIELDS = {
    "wagers": "handle",
    "patron_winnings": "payouts",
    "unadjusted_monthly_gaming": "gross_revenue",
    "promotional_deduction_6": "promo_credits",
    "total_gross_gaming": "net_revenue",
    "payment_7": "tax_paid",
    "federal_excise_tax_4": "federal_excise_tax",
}


class CTScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("CT")

    def discover_periods(self) -> list[dict]:
        """Return one 'period' per dataset (online + retail)."""
        return [
            {
                "dataset_id": CT_ONLINE_DATASET,
                "channel": "online",
                "period_end": date.today(),
                "period_type": "monthly",
            },
            {
                "dataset_id": CT_RETAIL_DATASET,
                "channel": "retail",
                "period_end": date.today(),
                "period_type": "monthly",
            },
        ]

    def download_report(self, period_info: dict) -> Path:
        """Fetch all data from the Socrata API and save as raw JSON."""
        dataset_id = period_info["dataset_id"]
        channel = period_info["channel"]
        api_url = f"{CT_API_BASE}/{dataset_id}.json?$limit=5000&$order=month_ending"

        filename = f"CT_{channel}_{dataset_id}.json"
        save_path = self.raw_dir / filename

        try:
            resp = fetch_with_retry(api_url)
            data = resp.json()
            with open(save_path, "w") as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"  Downloaded {channel}: {len(data)} rows → {filename}")
            return save_path
        except Exception as e:
            self.logger.error(f"  Failed to fetch {channel} data: {e}")
            raise

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse a CT Socrata JSON file into standardized rows."""
        channel = period_info["channel"]
        field_map = ONLINE_FIELDS if channel == "online" else RETAIL_FIELDS

        with open(file_path) as f:
            data = json.load(f)

        if not data:
            return pd.DataFrame()

        # Source provenance
        dataset_id = period_info.get("dataset_id", "")
        source_url = period_info.get('download_url', period_info.get('url', None))
        source_file = f"{CT_API_BASE}/{dataset_id}.json" if dataset_id else file_path.name

        # Build header list from the union of all record keys (for source_context)
        ctx_headers = list(data[0].keys())[:10] if data else []

        all_rows = []
        for row_idx, record in enumerate(data, start=1):
            # Parse date
            raw_date = record.get("month_ending", "")
            try:
                period_end = pd.to_datetime(raw_date).date()
            except Exception:
                continue

            # Parse operator
            operator_raw = record.get("licensee", "").strip()
            if not operator_raw:
                continue

            row = {
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": operator_raw,
                "channel": channel,
            }

            # Map financial fields
            for src_field, dst_field in field_map.items():
                val = record.get(src_field)
                if val is not None:
                    try:
                        row[dst_field] = float(val)
                    except (ValueError, TypeError):
                        row[dst_field] = None

            # Build source_context: 2 records before and after the current one
            idx = row_idx - 1  # 0-based index into data list
            start = max(0, idx - 2)
            end = min(len(data), idx + 3)
            ctx_rows = [
                [str(data[j].get(h, "")) for h in ctx_headers]
                for j in range(start, end)
            ]
            highlight = idx - start
            source_context = json.dumps({"headers": ctx_headers, "rows": ctx_rows, "highlight": highlight})

            # Source provenance fields
            row["source_file"] = source_file
            row["source_row"] = row_idx
            row["source_url"] = source_url
            row["source_sheet"] = None
            row["source_page"] = None
            row["source_raw_line"] = str(record)
            row["source_context"] = source_context

            all_rows.append(row)

        if not all_rows:
            return pd.DataFrame()

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))

        self.logger.info(
            f"  Parsed {channel}: {len(result)} rows, "
            f"{result['operator_raw'].nunique()} operators, "
            f"range {result['period_end'].min().date()} to {result['period_end'].max().date()}"
        )
        return result


if __name__ == "__main__":
    scraper = CTScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"CT SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Period types: {df['period_type'].value_counts().to_dict()}")
        print(f"Operators: {df['operator_standard'].nunique()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        print(f"\nPer-operator row counts:")
        for op in sorted(df['operator_standard'].unique()):
            count = len(df[df['operator_standard'] == op])
            print(f"  {op}: {count}")

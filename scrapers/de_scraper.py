"""
Delaware Sports Lottery Scraper
Source: delottery.com HTML tables (one page per fiscal year)
Format: HTML table
Launch: June 2018
Tax: 50% of net proceeds to state
Note: 3 casino sportsbooks + retailers; retail only; no online sports betting
"""

import sys
import re
import json
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger

DE_FY_URL = "https://www.delottery.com/Sports-Lottery/Sportsbooks/Monthly-Proceeds-And-Distribution-Financial-Year/{fy}"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# DE fiscal years run Jul-Jun
# FY2019 = Jul 2018 - Jun 2019 (first full FY with sports betting)
DE_START_FY = 2019
DE_CURRENT_FY = 2026


class DEScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("DE")

    def discover_periods(self) -> list[dict]:
        """Generate FY URLs."""
        periods = []
        for fy in range(DE_START_FY, DE_CURRENT_FY + 1):
            periods.append({
                "period_end": date(fy, 6, 30),
                "period_type": "fy_page",
                "fy": fy,
            })
        return periods

    def download_report(self, period_info: dict) -> Path:
        """Download (save) DE FY HTML page."""
        fy = period_info["fy"]
        filename = f"DE_FY{fy}.html"
        save_path = self.raw_dir / filename

        if save_path.exists() and save_path.stat().st_size > 1000:
            return save_path

        url = DE_FY_URL.format(fy=fy)
        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=30)

        if resp.status_code != 200:
            raise FileNotFoundError(f"DE FY{fy} page not found (status {resp.status_code})")

        with open(save_path, "w") as f:
            f.write(resp.text)

        self.logger.info(f"  Downloaded: {filename}")
        return save_path

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse DE FY HTML table."""
        with open(file_path, "r") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            return pd.DataFrame()

        table = tables[0]
        all_trs = table.find_all("tr")
        if not all_trs:
            return pd.DataFrame()

        # Build header for source_context
        header_cells = [th.get_text(strip=True) for th in all_trs[0].find_all(['th', 'td'])] if all_trs else []

        all_rows = []
        current_period_end = None

        for row_idx, tr in enumerate(all_trs):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 5:
                continue

            raw_row_text = tr.get_text(separator=" | ", strip=True)

            # Skip fiscal year total rows
            if cells[0] and "fiscal year" in cells[0].lower():
                current_period_end = None  # Stop parsing until next date range
                continue

            # Check if first cell has a date range like "07/01/25 - 07/27/25"
            date_match = re.match(r'(\d{2})/(\d{2})/(\d{2})\s*-\s*(\d{2})/(\d{2})/(\d{2})', cells[0])
            if date_match:
                end_month = int(date_match.group(4))
                end_day = int(date_match.group(5))
                end_yy = int(date_match.group(6))
                end_year = 2000 + end_yy
                # Use end-of-month as period_end
                last_day = calendar.monthrange(end_year, end_month)[1]
                current_period_end = date(end_year, end_month, last_day)

            if current_period_end is None:
                continue

            # Check data type in column 1
            data_type = cells[1].strip().lower() if len(cells) > 1 else ""

            if "sports sales" in data_type:
                # Handle/Sales row: Delaware Park, Bally's Dover, Harrington, Retailers, Total
                # Columns: date, type, DP, (empty), Bally, (empty), Harr, (empty), Retail, (empty), Total
                operators = self._extract_operator_values(cells)
                # Build source_context: header + 2 rows before/after
                start = max(1, row_idx - 2)
                end = min(len(all_trs), row_idx + 3)
                ctx_rows = []
                highlight = None
                for j in range(start, end):
                    ctx_cells = [td.get_text(strip=True) for td in all_trs[j].find_all(['th', 'td'])]
                    ctx_rows.append(ctx_cells[:10])
                    if j == row_idx:
                        highlight = len(ctx_rows) - 1
                source_context = json.dumps({"headers": header_cells[:10], "rows": ctx_rows, "highlight": highlight})
                for op_name, value in operators:
                    handle = self._parse_money(value)
                    if handle is not None:
                        all_rows.append({
                            "period_end": current_period_end,
                            "period_type": "monthly",
                            "operator_raw": op_name,
                            "channel": "retail",
                            "handle": handle,
                            "source_raw_line": raw_row_text,
                            "source_context": source_context,
                        })

            elif "amount won" in data_type:
                # Payouts row — only Total column has data, per-operator is blank
                total_val = cells[10] if len(cells) > 10 else cells[-1]
                payouts = self._parse_money(total_val)
                if payouts is not None and current_period_end:
                    # Distribute payouts to operators proportionally by handle
                    period_rows = [r for r in all_rows if r["period_end"] == current_period_end]
                    total_handle = sum(r.get("handle", 0) or 0 for r in period_rows)
                    for row in period_rows:
                        h = row.get("handle", 0) or 0
                        if total_handle > 0 and h > 0:
                            op_payouts = payouts * (h / total_handle)
                            row["payouts"] = op_payouts
                            row["standard_ggr"] = h - op_payouts
                            # Append the amount won raw line
                            existing_raw = row.get("source_raw_line", "")
                            row["source_raw_line"] = existing_raw + " ||| " + raw_row_text if existing_raw else raw_row_text

            elif "net proceeds" in data_type or "net revenue" in data_type:
                operators = self._extract_operator_values(cells)
                for op_name, value in operators:
                    revenue = self._parse_money(value)
                    if revenue is not None:
                        # Find existing row for this operator+period and add revenue
                        found = False
                        for row in all_rows:
                            if (row["period_end"] == current_period_end and
                                row["operator_raw"] == op_name):
                                row["net_revenue"] = revenue
                                # Append the net proceeds raw line
                                existing_raw = row.get("source_raw_line", "")
                                row["source_raw_line"] = existing_raw + " ||| " + raw_row_text if existing_raw else raw_row_text
                                found = True
                                break
                        if not found:
                            # Build source_context for net proceeds fallback row
                            start_np = max(1, row_idx - 2)
                            end_np = min(len(all_trs), row_idx + 3)
                            ctx_rows_np = []
                            highlight_np = None
                            for j in range(start_np, end_np):
                                ctx_cells_np = [td.get_text(strip=True) for td in all_trs[j].find_all(['th', 'td'])]
                                ctx_rows_np.append(ctx_cells_np[:10])
                                if j == row_idx:
                                    highlight_np = len(ctx_rows_np) - 1
                            source_context_np = json.dumps({"headers": header_cells[:10], "rows": ctx_rows_np, "highlight": highlight_np})
                            all_rows.append({
                                "period_end": current_period_end,
                                "period_type": "monthly",
                                "operator_raw": op_name,
                                "channel": "retail",
                                "net_revenue": revenue,
                                "source_raw_line": raw_row_text,
                                "source_context": source_context_np,
                            })

        if not all_rows:
            return pd.DataFrame()

        # gross_revenue = state's reported "Net Proceeds" (after revenue share split)
        # This is the number DE publishes; standard_ggr is our handle - payouts metric
        for row in all_rows:
            if "net_revenue" in row and row.get("net_revenue") is not None:
                row["gross_revenue"] = row["net_revenue"]

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))

        # Source provenance
        result["source_file"] = file_path.name
        source_url = period_info.get('download_url', period_info.get('url', None))
        if source_url is None:
            fy = period_info.get("fy")
            if fy:
                source_url = DE_FY_URL.format(fy=fy)
        result["source_url"] = source_url

        return result

    def _extract_operator_values(self, cells: list) -> list[tuple]:
        """Extract operator name-value pairs from a table row."""
        # Header: Accounting Month, Sports Data Type, Delaware Park, "", Bally's Dover, "", Harrington, "", Retailers, "", Total
        # So operator values are at indices 2, 4, 6, 8 (skipping empty cols)
        operators = []
        op_names = ["Delaware Park", "Bally's Dover", "Harrington Raceway", "DE Retailers"]
        # Values at positions 2, 4, 6, 8
        indices = [2, 4, 6, 8]
        for i, idx in enumerate(indices):
            if idx < len(cells) and i < len(op_names):
                val = cells[idx]
                if val and val != "":
                    operators.append((op_names[i], val))
        return operators

    def _parse_money(self, value) -> float | None:
        """Parse money from DE HTML."""
        if value is None:
            return None
        s = str(value).strip().replace('$', '').replace(',', '').strip()
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        if not s or s in ('-', 'N/A', ''):
            return None
        try:
            return float(s)
        except ValueError:
            return None


if __name__ == "__main__":
    scraper = DEScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"DE SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Operators: {df['operator_standard'].nunique()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")

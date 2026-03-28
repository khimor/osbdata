"""
West Virginia Sports Wagering Scraper
Source: WV Lottery Commission (business.wvlottery.com)
Format: ZIP containing XLSX per fiscal year (July-June), weekly data
Launch: August 2018
Tax: 10% on Taxable Receipts
Note: 5 casino venues + Total sheet; retail + mobile split in columns; weekly data
"""

import sys
import re
import io
import zipfile
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, fetch_with_retry

# Column layout for WV sheets (0-indexed)
# Retail: B-E (cols 1-4), Mobile: G-J (cols 6-9), Total: L-O (cols 11-14)
# Spacers: F(5), K(10), P(15)
# Tax cols: Q(16), R(17), S(18)
#
# Per channel: Gross Tickets Written | Voids | Tickets Cashed | Taxable Receipts
# GGR (standard_ggr) = Gross Tickets Written - Voids - Tickets Cashed = Taxable Receipts
# Payouts = abs(Tickets Cashed) (stored as negative in source)
CHANNELS = {
    "retail": {"handle_col": 1, "voids_col": 2, "cashed_col": 3, "ggr_col": 4, "tax_col": 16},
    "online": {"handle_col": 6, "voids_col": 7, "cashed_col": 8, "ggr_col": 9, "tax_col": None},
}

WV_ZIP_URL = "https://assets.ctfassets.net/nm98451qj5dg/4L2FOWsQNspNvc7sfCNAPi/7c2d78c740a87908156dcf4a73fc6eed/Sports_Wagering.zip"
WV_PAGE_URL = "https://business.wvlottery.com/resourcesPayments"

VENUE_SHEETS = ["Mountaineer", "Wheeling", "Mardi Gras", "Charles Town", "Greenbrier"]


class WVScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("WV")

    def discover_periods(self) -> list[dict]:
        """Return a single period pointing to the ZIP download."""
        # Try to find the current ZIP URL from the page
        zip_url = self._find_zip_url()
        if not zip_url:
            zip_url = WV_ZIP_URL

        return [{
            "download_url": zip_url,
            "period_end": date.today(),
            "period_type": "weekly",
        }]

    def _find_zip_url(self) -> str | None:
        """Try to find the Sports Wagering ZIP URL from the WV Lottery page."""
        try:
            resp = fetch_with_retry(WV_PAGE_URL)
            # Look for ZIP links in the page
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "sports_wagering" in href.lower() and ".zip" in href.lower():
                    self.logger.info(f"  Found ZIP URL: {href}")
                    return href
                if "sport" in link.get_text(strip=True).lower() and ".zip" in href.lower():
                    return href
        except Exception as e:
            self.logger.warning(f"  Could not fetch WV page: {e}")
        return None

    def download_report(self, period_info: dict) -> Path:
        """Download and extract the WV Sports Wagering ZIP."""
        url = period_info["download_url"]
        zip_path = self.raw_dir / "Sports_Wagering.zip"

        # Always re-download since it's updated
        resp = fetch_with_retry(url)
        with open(zip_path, "wb") as f:
            f.write(resp.content)
        self.logger.info(f"  Downloaded ZIP: {zip_path.stat().st_size:,} bytes")

        # Extract all XLSX files
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith(".xlsx"):
                    zf.extract(name, self.raw_dir)
                    self.logger.info(f"  Extracted: {name}")

        return self.raw_dir

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse all extracted WV XLSX files (one per FY)."""
        all_rows = []
        source_url = period_info.get('download_url', period_info.get('url', None))

        xlsx_files = sorted(file_path.glob("*.xlsx"))
        if not xlsx_files:
            # Check for files in subdirectory
            xlsx_files = sorted(file_path.glob("**/*.xlsx"))

        for xlsx_file in xlsx_files:
            if "~$" in xlsx_file.name:
                continue
            rows = self._parse_fy_file(xlsx_file)
            all_rows.extend(rows)

        if not all_rows:
            return pd.DataFrame()

        # Add source_url (same ZIP download for all rows)
        for row in all_rows:
            row["source_url"] = source_url

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])

        # period_start = period_end - 6 days for weekly
        from datetime import timedelta
        result["period_start"] = result["period_end"] - timedelta(days=6)

        self.logger.info(
            f"  Parsed WV: {len(result)} rows, "
            f"{result['operator_raw'].nunique()} venues, "
            f"range {result['period_end'].min().date()} to {result['period_end'].max().date()}"
        )
        return result

    def _parse_fy_file(self, file_path: Path) -> list[dict]:
        """Parse a single WV FY XLSX file."""
        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return []

        rows = []

        # Parse venue sheets (skip Total to avoid double counting)
        for sheet_name in xls.sheet_names:
            if sheet_name.strip().lower() == "total":
                continue
            if sheet_name.strip() not in VENUE_SHEETS:
                continue

            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            venue_rows = self._parse_venue_sheet(df, sheet_name.strip(), file_path)
            rows.extend(venue_rows)

        return rows

    def _parse_venue_sheet(self, df: pd.DataFrame, venue_name: str, file_path: Path = None) -> list[dict]:
        """Parse a venue sheet for weekly retail + mobile data."""
        rows = []
        source_file = file_path.name if file_path else None

        # Find header row for source_context (look for "Gross Tickets" or "Retail" in any cell)
        header_row_idx = 0
        for _h in range(min(10, len(df))):
            row_text = ' '.join(str(v) for v in df.iloc[_h] if pd.notna(v)).lower()
            if "gross tickets" in row_text or "taxable receipts" in row_text:
                header_row_idx = _h
                break

        for i in range(len(df)):
            date_val = df.iloc[i, 0]  # Column A: date

            if pd.isna(date_val):
                continue

            # Parse date (can be datetime or string with asterisk)
            period_end = None
            if isinstance(date_val, datetime):
                period_end = date_val.date()
            elif isinstance(date_val, date):
                period_end = date_val
            elif isinstance(date_val, str):
                # Clean asterisks and whitespace
                cleaned = date_val.strip().rstrip("*").strip()
                if not cleaned:
                    continue
                try:
                    period_end = pd.to_datetime(cleaned).date()
                except Exception:
                    continue
            else:
                continue

            # Skip if not a valid date or too old/future
            if period_end is None or period_end > date.today():
                continue
            if period_end.year < 2018:
                continue

            # Parse retail and mobile data
            for channel, cols in CHANNELS.items():
                handle = self._parse_money(df.iloc[i, cols["handle_col"]])
                ggr = self._parse_money(df.iloc[i, cols["ggr_col"]])

                if handle is None and ggr is None:
                    continue

                # Skip if handle is 0 and GGR is 0 (no activity)
                if (handle == 0 or handle is None) and (ggr == 0 or ggr is None):
                    continue

                # Capture raw cell values from this Excel row
                raw_cells = [str(v) for c in range(min(19, df.shape[1]))
                             if pd.notna(v := df.iloc[i, c]) and str(v).strip()]

                source_context = self.build_source_context(df, header_row_idx, i, context_rows=2, max_cols=10)

                row = {
                    "period_end": period_end,
                    "period_type": "weekly",
                    "operator_raw": venue_name,
                    "channel": channel,
                    "handle": handle,
                    "gross_revenue": ggr,     # Taxable Receipts = GGR
                    "standard_ggr": ggr,      # GGR = Gross Tickets - Voids - Cashed
                    "source_file": source_file,
                    "source_sheet": venue_name,
                    "source_row": i + 1,      # 1-indexed Excel row
                    "source_raw_line": ' | '.join(raw_cells),
                    "source_context": source_context,
                }

                # Derive payouts from Tickets Cashed + Voids
                voids = self._parse_money(df.iloc[i, cols["voids_col"]])
                cashed = self._parse_money(df.iloc[i, cols["cashed_col"]])
                if cashed is not None:
                    # Cashed and Voids are negative in source; payouts = abs(cashed + voids)
                    payouts = abs(cashed) + (abs(voids) if voids else 0)
                    row["payouts"] = payouts

                # Tax (only on retail channel's tax col, covers both channels)
                tax_col = cols.get("tax_col")
                if tax_col is not None:
                    tax = self._parse_money(df.iloc[i, tax_col])
                    if tax is not None:
                        row["tax_paid"] = tax

                rows.append(row)

        return rows

    def _parse_money(self, value) -> float | None:
        """Parse a money value from Excel cell."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip().replace('$', '').replace(',', '').replace(' ', '')
        if not s or s in ('-', 'N/A', '', '#DIV/0!', '#REF!'):
            return None
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        try:
            return float(s)
        except ValueError:
            return None


if __name__ == "__main__":
    scraper = WVScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"WV SCRAPER RESULTS")
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

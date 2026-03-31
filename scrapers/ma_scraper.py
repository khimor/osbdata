"""
Massachusetts Sports Wagering Scraper
Source: massgaming.com individual operator revenue PDFs
Format: PDF (one per operator, contains full monthly history as table)
Launch: March 2023
Tax: 20% on AGSWR (Adjusted Gross Sports Wagering Receipts)
Note: Operator-level detail; Cat.1=retail, Cat.3=online; each PDF has all months
"""

import sys
import re
import io
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger

MA_REVENUE_URL = "https://massgaming.com/regulations/revenue/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}


class MAScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("MA")

    def discover_periods(self) -> list[dict]:
        """Discover operator PDF download links from MA revenue page."""
        periods = []

        resp = requests.get(MA_REVENUE_URL, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)

            if ".pdf" not in href.lower():
                continue
            if "sport" not in text.lower() and "sport" not in href.lower():
                continue
            if "wagering" not in text.lower() and "wagering" not in href.lower():
                continue
            if "revenue" not in text.lower():
                continue

            # Extract operator name and category from text
            # e.g. "DraftKings (Cat. 3) Sports Wagering Revenue Report – February 2026"
            op_match = re.match(r'(.+?)\s*\(Cat\.\s*(\d)\)', text)
            if not op_match:
                continue

            operator_name = op_match.group(1).strip()
            category = int(op_match.group(2))
            channel = "retail" if category == 1 else "online"

            periods.append({
                "download_url": href,
                "operator_name": operator_name,
                "channel": channel,
                "category": category,
                "period_type": "operator_file",
                "period_end": date.today(),  # placeholder, actual dates from PDF
            })

        self.logger.info(f"  Found {len(periods)} operator PDFs")
        return periods

    def download_report(self, period_info: dict) -> Path:
        """Download MA operator PDF."""
        url = period_info["download_url"]
        operator = period_info["operator_name"].replace(" ", "_").replace("/", "_")
        cat = period_info["category"]
        filename = f"MA_{operator}_cat{cat}.pdf"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            return save_path

        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
        if resp.status_code != 200:
            raise FileNotFoundError(f"MA PDF not found: {url} (status {resp.status_code})")

        with open(save_path, "wb") as f:
            f.write(resp.content)

        self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
        return save_path

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse MA operator PDF — extract monthly rows from table."""
        operator_name = period_info["operator_name"]
        channel = period_info["channel"]

        # Source provenance
        source_file = file_path.name
        source_url = period_info.get('download_url', period_info.get('url', None))

        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        all_rows = []

        for page in pdf.pages:
            page_number = page.page_number  # 1-indexed
            tables = page.extract_tables()
            if not tables:
                continue

            for table_idx, table in enumerate(tables):
                for row in table:
                    if not row or not row[0]:
                        continue

                    cell0 = str(row[0]).strip()

                    # Match month/year pattern: "March 2023", "January 2024", etc.
                    # Also handles "July 2025 FY26" where fiscal year tag is appended
                    m = re.match(r'^(\w+)\s+(\d{4})\b', cell0)
                    if not m:
                        continue

                    month_name = m.group(1).lower()
                    year = int(m.group(2))
                    if month_name not in MONTH_NAMES:
                        continue

                    month_num = MONTH_NAMES[month_name]
                    last_day = calendar.monthrange(year, month_num)[1]
                    period_end = date(year, month_num, last_day)

                    # Extract values from columns
                    # Columns: Month, Ticket Write, Handle, Win, Hold%, Fed Excise, Taxable AGSWR, Tax
                    handle = self._parse_money(row[2] if len(row) > 2 else None)
                    # Handle might be split with Win in same cell
                    if handle is None and len(row) > 1:
                        handle = self._parse_money(row[1])

                    win = self._parse_money(row[3] if len(row) > 3 else None)
                    if win is None and len(row) > 2:
                        # Sometimes handle+win merged
                        parts = str(row[2] or "").split("$")
                        if len(parts) >= 3:
                            handle = self._parse_money(parts[1])
                            win = self._parse_money(parts[2])

                    taxable = self._parse_money(row[6] if len(row) > 6 else None)
                    tax = self._parse_money(row[7] if len(row) > 7 else None)

                    if handle is None and win is None and taxable is None:
                        continue

                    all_rows.append({
                        "period_end": period_end,
                        "period_type": "monthly",
                        "operator_raw": operator_name,
                        "channel": channel,
                        "handle": handle,
                        "gross_revenue": win,
                        "net_revenue": taxable,
                        "tax_paid": tax,
                        "source_file": source_file,
                        "source_url": source_url,
                        "source_page": page_number,
                        "source_table_index": table_idx,
                        "source_raw_line": ' | '.join(str(c) for c in row if c),
                    })

        pdf.close()

        if not all_rows:
            return pd.DataFrame()

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        return result

    def _parse_money(self, value) -> float | None:
        """Parse money from MA PDF."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip().replace('$', '').replace(',', '').strip()
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        if not s or s in ('-', 'N/A', '', 'None'):
            return None
        try:
            return float(s)
        except ValueError:
            return None


if __name__ == "__main__":
    scraper = MAScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"MA SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Operators: {df['operator_standard'].nunique()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")

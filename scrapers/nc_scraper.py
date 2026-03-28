"""
North Carolina Sports Betting Revenue Scraper
Source: ncgaming.gov monthly PDF reports
Format: PDF (aggregate monthly data, no operator breakdown)
Launch: March 2024
Tax: 18% on gross wagering revenue
Note: Online only (mobile sports wagering); aggregate data; multiple FYs per PDF
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

NC_REPORTS_URL = "https://ncgaming.gov/reports/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    # Abbreviations used in NC PDFs
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sept": 9, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


class NCScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("NC")

    def discover_periods(self) -> list[dict]:
        """Discover PDF download links from NC reports page."""
        periods = []

        resp = requests.get(NC_REPORTS_URL, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")

        seen_urls = set()
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            if ".pdf" not in href.lower():
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Only grab monthly revenue/report PDFs (NCSLC Monthly reports)
            href_lower = href.lower()
            if "ncslc" not in href_lower and "monthly" not in href_lower:
                continue
            if "rules" in href_lower or "manual" in href_lower:
                continue

            # Extract month from URL
            period_end = self._extract_date(href)
            if period_end:
                periods.append({
                    "download_url": href,
                    "period_end": period_end,
                    "period_type": "monthly",
                })

        self.logger.info(f"  Found {len(periods)} monthly reports")
        return periods

    def _extract_date(self, url: str) -> date | None:
        """Extract month/year from NC PDF URL.

        Extracts from the filename portion (after last /) to avoid confusing
        the upload directory year (/2025/08/) with the data year (_2024.pdf).
        """
        # Use filename only to avoid URL path year confusion
        filename = url.split("/")[-1].lower()

        for month_name, month_num in MONTH_NAMES.items():
            short = month_name[:3]
            # Match full name, or short form (3+ chars) after a separator
            if month_name in filename or re.search(rf'[\-_]{short}[a-z]*[\-_.\d]', filename):
                # Find 4-digit year in the filename
                year_match = re.search(r'(\d{4})', filename)
                if year_match:
                    year = int(year_match.group(1))
                    last_day = calendar.monthrange(year, month_num)[1]
                    return date(year, month_num, last_day)
        return None

    def download_report(self, period_info: dict) -> Path:
        """Download NC monthly PDF."""
        url = period_info["download_url"]
        period_end = period_info["period_end"]
        filename = f"NC_{period_end.year}_{period_end.month:02d}.pdf"
        save_path = self.raw_dir / filename

        if save_path.exists() and save_path.stat().st_size > 5000:
            return save_path

        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
        if resp.status_code != 200:
            raise FileNotFoundError(f"NC PDF not found: {url} (status {resp.status_code})")

        with open(save_path, "wb") as f:
            f.write(resp.content)

        self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
        return save_path

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse NC monthly PDF — extract ALL months from the cumulative FY table.

        NC PDFs are cumulative: each month's report contains all prior months
        in the fiscal year. We extract every month with data and let the base
        scraper's deduplication (keep last) ensure the latest/most complete
        values win.
        """
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
            text = page.extract_text() or ""
            if not text:
                continue

            lines = text.splitlines()
            # Determine which FY this page covers
            fy_match = re.search(r'FY\s*(\d{4})', text)

            for line in lines:
                line_stripped = line.strip()

                # Skip headers, totals, footnotes
                if not line_stripped or line_stripped.startswith("Month") or "Total" in line_stripped:
                    continue
                if line_stripped[0].isdigit() and "resettlement" in line_stripped.lower():
                    continue
                if "preliminary" in line_stripped.lower() or "revenue" in line_stripped.lower():
                    if not line_stripped[0].isalpha() or line_stripped[0] == "$":
                        continue

                # Match month row: "July $361,541,492 $8,844,097 $370,385,588 ..."
                parts = line_stripped.split()
                if not parts:
                    continue

                # First word should be a month name (with optional footnote/asterisk)
                month_word = re.sub(r'[\d*]+$', '', parts[0]).lower()
                if month_word not in MONTH_NAMES:
                    continue

                month_num = MONTH_NAMES[month_word]

                # Determine year from FY context
                if fy_match:
                    fy = int(fy_match.group(1))
                    # NC FY runs Jul-Jun: Jul-Dec = FY year - 1, Jan-Jun = FY year
                    if month_num >= 7:
                        year = fy - 1
                    else:
                        year = fy
                else:
                    continue

                # Extract dollar values — skip months with no data (blank rows)
                dollars = re.findall(r'\$[\d,]+', line_stripped)
                if len(dollars) < 5:
                    continue

                last_day = calendar.monthrange(year, month_num)[1]
                period_end = date(year, month_num, last_day)

                handle = self._parse_money(dollars[0])        # Paid Wagering Revenue
                promo = self._parse_money(dollars[1])          # Promo Wagering Revenue
                total_handle = self._parse_money(dollars[2])   # Total Wagering Revenue
                winnings = self._parse_money(dollars[4])       # Amounts Paid as Winnings
                ggr = self._parse_money(dollars[5]) if len(dollars) > 5 else None  # Gross Wagering Revenue
                tax = self._parse_money(dollars[6]) if len(dollars) > 6 else None  # Estimated Tax

                if handle is None and ggr is None:
                    continue

                all_rows.append({
                    "period_end": period_end,
                    "period_type": "monthly",
                    "operator_raw": "ALL",
                    "channel": "online",
                    "handle": total_handle,
                    "gross_revenue": ggr,
                    "standard_ggr": ggr,  # Gross Wagering Revenue = handle - payouts
                    "payouts": (total_handle - ggr) if total_handle and ggr else None,
                    "promo_credits": promo,
                    "tax_paid": tax,
                    "source_file": source_file,
                    "source_url": source_url,
                    "source_page": page_number,
                    "source_table_index": 0,
                    "source_raw_line": line_stripped,
                })

        pdf.close()

        if not all_rows:
            return pd.DataFrame()

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        return result

    def _parse_money(self, value) -> float | None:
        """Parse money from NC PDF."""
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
    scraper = NCScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"NC SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")

"""
New Hampshire Sports Betting Revenue Scraper
Source: nhlottery.com yearly Sports Betting Summary PDFs
Format: PDF (monthly rows within fiscal year pages)
Launch: December 2019
Tax: 51% revenue share to state (DraftKings sole operator)
Note: Single operator (DraftKings via NH Lottery). Online + retail. FY runs Jul-Jun.
"""

import sys
import re
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

NH_FINANCIAL_URL = "https://www.nhlottery.com/About-Us/Financial-Reports"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# NH FY runs Jul-Jun. Sports betting launched Dec 2019 (mid FY20).
NH_START_FY = 2020
NH_CURRENT_FY = 2026

MONTH_ABBREVS = {
    "jul": 7, "aug": 8, "sep": 9, "oct": 10,
    "nov": 11, "dec": 12, "jan": 1, "feb": 2,
    "mar": 3, "apr": 4, "may": 5, "jun": 6,
}


class NHScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("NH")

    def discover_periods(self) -> list[dict]:
        """
        Discover Sports Betting Summary PDF links from the NH Lottery
        financial reports page. Each PDF covers one fiscal year with
        monthly rows inside.
        """
        periods = []

        resp = requests.get(NH_FINANCIAL_URL, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        seen_urls = set()
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            if ".pdf" not in href.lower():
                continue
            if "sports" not in href.lower() and "sports" not in text:
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Extract FY from URL or link text, e.g. "Sports_Betting_Summary_FY26.pdf"
            fy_match = re.search(r'FY\s*(\d{2,4})', href, re.IGNORECASE)
            if not fy_match:
                fy_match = re.search(r'FY\s*(\d{2,4})', text, re.IGNORECASE)
            if not fy_match:
                continue

            fy_raw = int(fy_match.group(1))
            fy = fy_raw if fy_raw > 100 else 2000 + fy_raw

            # Each PDF is one FY page -- we'll extract monthly rows in parse_report.
            # Use end of FY as period_end for discovery purposes.
            periods.append({
                "download_url": href,
                "period_end": date(fy, 6, 30),
                "period_type": "fy_page",
                "fy": fy,
            })

        self.logger.info(f"  Found {len(periods)} NH sports betting FY PDFs")
        return periods

    def download_report(self, period_info: dict) -> Path:
        """Download an NH Sports Betting Summary PDF."""
        url = period_info["download_url"]
        fy = period_info["fy"]
        filename = f"NH_FY{fy}.pdf"
        save_path = self.raw_dir / filename

        if save_path.exists() and save_path.stat().st_size > 1000:
            return save_path

        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=60)
        if resp.status_code != 200:
            raise FileNotFoundError(
                f"NH PDF not found: {url} (status {resp.status_code})"
            )

        with open(save_path, "wb") as f:
            f.write(resp.content)

        self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
        return save_path

    @staticmethod
    def _fix_split_numbers(text: str) -> str:
        """Fix numbers split by pdfplumber column extraction.

        pdfplumber sometimes splits large numbers at PDF column boundaries,
        e.g. "$45,397,482" is extracted as "$ 4 5,397,482" (the leading
        digit(s) separated by a space). This also happens without a dollar
        sign at the start of a section, e.g. "1 03,390,747".

        We detect the split by looking for a short digit group followed by
        a space and then another digit (which would be invalid formatting
        for two separate comma-formatted numbers sitting next to each other).
        """
        # Fix split after dollar sign: '$ 4 5,397,482' -> '$45,397,482'
        text = re.sub(r'([$]\s*)(\d{1,3})\s+(\d)', r'\1\2\3', text)
        # Fix split at start of string (no dollar sign): '1 03,390,747' -> '103,390,747'
        text = re.sub(r'^(\d{1,3})\s+(\d{2},)', r'\1\2', text)
        return text

    @staticmethod
    def _parse_section_values(section_text: str) -> tuple:
        """Parse handle, GGR, and rev_share from one column-group section.

        Each section contains three values. Returns a tuple of
        (handle, ggr, rev_share) or (None, None, None) if the section
        is empty (e.g. retail before it launched, or future months).
        """
        section_text = section_text.strip()
        if not section_text or all(c in '$- ' for c in section_text):
            return None, None, None

        # Remove trailing 'x' markers present in some FY2023 rows
        section_text = re.sub(r'\s*x\s*$', '', section_text)

        # Fix numbers that pdfplumber split across column boundaries
        section_text = NHScraper._fix_split_numbers(section_text)

        # Extract numeric values, including negative values in parentheses
        values = []
        for m in re.finditer(r'\([\d,]+(?:\.\d+)?\)|[\d,]+(?:\.\d+)?', section_text):
            v = m.group()
            if v.startswith('(') and v.endswith(')'):
                v = '-' + v[1:-1]
            v = v.replace(',', '')
            try:
                values.append(float(v))
            except ValueError:
                continue

        if len(values) >= 3:
            return values[0], values[1], values[2]
        return None, None, None

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """
        Parse NH Sports Betting Summary PDF.

        The PDF has three column groups side by side:
          Mobile Sports Betting | Retail Sports Betting | Combined
        Each group has: Handle, GGR, State Rev Share

        Each data line repeats the month label before each group, e.g.:
          Jul-25 $ 44,191,929 $ 4,880,547 $ 2,115,717 Jul-25 $ 3,201,185 ...

        pdfplumber sometimes splits large numbers at column boundaries
        (e.g. "$45,397,482" becomes "$ 4 5,397,482"). We handle this by
        splitting each line into its three sections first, then fixing
        split numbers within each section before extracting values.
        """
        fy = period_info["fy"]

        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        all_rows = []

        for page_idx, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            if not text:
                continue
            page_num = page_idx + 1  # 1-indexed

            lines = text.splitlines()
            for line in lines:
                line_stripped = line.strip()
                if not line_stripped:
                    continue

                # Match lines starting with month abbreviation and 2-digit year
                # e.g., "Jul-25 $ 44,191,929 ..."
                month_match = re.match(
                    r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})\b',
                    line_stripped, re.IGNORECASE
                )
                if not month_match:
                    continue

                month_abbr = month_match.group(1).lower()[:3]
                year_2d = int(month_match.group(2))
                month_num = MONTH_ABBREVS.get(month_abbr)
                if month_num is None:
                    continue

                year = 2000 + year_2d
                last_day = calendar.monthrange(year, month_num)[1]
                period_end = date(year, month_num, last_day)

                # Split the line into its three sections (Mobile, Retail, Combined)
                # by splitting on the repeated month label (e.g. "Jul-24")
                month_label = month_match.group(0)  # e.g. "Jul-24"
                sections = re.split(re.escape(month_label) + r'\s*', line_stripped)
                # Filter out empty strings from the split
                sections = [s for s in sections if s.strip()]

                # Parse each section for its 3 values (handle, ggr, rev_share)
                if len(sections) >= 3:
                    mobile_vals = self._parse_section_values(sections[0])
                    retail_vals = self._parse_section_values(sections[1])
                    # combined_vals = self._parse_section_values(sections[2])
                elif len(sections) == 2:
                    # FY2020 early months: no retail data, empty section between labels
                    mobile_vals = self._parse_section_values(sections[0])
                    retail_vals = (None, None, None)
                    # combined_vals = self._parse_section_values(sections[1])
                elif len(sections) == 1:
                    mobile_vals = self._parse_section_values(sections[0])
                    retail_vals = (None, None, None)
                    # combined_vals = (None, None, None)
                else:
                    continue

                # Skip months with no data (future months with dashes)
                if mobile_vals[0] is None and retail_vals[0] is None:
                    continue

                # Source provenance fields
                _src_file = file_path.name
                _src_url = period_info.get('download_url', period_info.get('url', None))

                # Add online row (if data exists)
                if mobile_vals[0] is not None:
                    all_rows.append({
                        "period_end": period_end,
                        "period_type": "monthly",
                        "operator_raw": "DraftKings (exclusive sole operator under state contract)",
                        "channel": "online",
                        "handle": mobile_vals[0],
                        "gross_revenue": mobile_vals[1],
                        "tax_paid": mobile_vals[2],
                        "source_file": _src_file,
                        "source_page": page_num,
                        "source_url": _src_url,
                        "source_raw_line": line_stripped,
                    })

                # Add retail row (if data exists)
                if retail_vals[0] is not None:
                    all_rows.append({
                        "period_end": period_end,
                        "period_type": "monthly",
                        "operator_raw": "DraftKings (exclusive sole operator under state contract)",
                        "channel": "retail",
                        "handle": retail_vals[0],
                        "gross_revenue": retail_vals[1],
                        "tax_paid": retail_vals[2],
                        "source_file": _src_file,
                        "source_page": page_num,
                        "source_url": _src_url,
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
        """Parse money from NH PDF."""
        if value is None:
            return None
        s = str(value).strip().replace('$', '').replace(',', '').replace(' ', '').strip()
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        if not s or s in ('-', 'N/A', ''):
            return None
        try:
            return float(s)
        except ValueError:
            return None


if __name__ == "__main__":
    scraper = NHScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"NH SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")

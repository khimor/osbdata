"""
Maryland Sports Wagering Scraper
Source: mdgaming.com monthly Excel reports
Format: Excel (.xlsx), one per month, downloaded from archive pages
Launch: December 2021 (retail), November 2022 (mobile)
Tax: 15% retail, 20% mobile on Taxable Win
Note: Operator-level detail + sport breakdown; 2 rows per operator (month + FYTD)
"""

import sys
import re
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, download_file

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MD_ARCHIVE_URLS = [
    "https://www.mdgaming.com/maryland-sports-wagering/revenue-reports/all-financial-reports/",
    "https://www.mdgaming.com/maryland-sports-wagering/revenue-reports/all-financial-reports/page/2/",
    "https://www.mdgaming.com/maryland-sports-wagering/revenue-reports/all-financial-reports/page/3/",
    "https://www.mdgaming.com/maryland-sports-wagering/revenue-reports/all-financial-reports/page/4/",
]


class MDScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("MD")

    def discover_periods(self) -> list[dict]:
        """Discover Excel download URLs from MD archive pages."""
        periods = []
        report_pages = set()

        for archive_url in MD_ARCHIVE_URLS:
            try:
                resp = requests.get(archive_url, headers={
                    "User-Agent": USER_AGENT,
                    "Accept-Encoding": "gzip, deflate",
                }, timeout=30)
                soup = BeautifulSoup(resp.text, "html.parser")

                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    text = link.get_text(strip=True).lower()
                    if ("revenue" in text or "sport" in text or "wagering" in text) and \
                       "mdgaming.com" in href and href not in report_pages:
                        report_pages.add(href)
            except Exception as e:
                self.logger.warning(f"  Failed to fetch {archive_url}: {e}")

        # For each report page, find the Excel download link
        for page_url in sorted(report_pages):
            xlsx_url = self._find_xlsx_on_page(page_url)
            if xlsx_url:
                period_end = self._extract_date(xlsx_url, page_url)
                if period_end:
                    periods.append({
                        "download_url": xlsx_url,
                        "period_end": period_end,
                        "period_type": "monthly",
                    })

        self.logger.info(f"  Found {len(periods)} Excel reports")
        return periods

    def _find_xlsx_on_page(self, page_url: str) -> str | None:
        """Find .xlsx download link on a report page."""
        try:
            resp = requests.get(page_url, headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
            }, timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if ".xlsx" in href.lower():
                    return href
        except Exception:
            pass
        return None

    def _extract_date(self, xlsx_url: str, page_url: str) -> date | None:
        """Extract month/year from URL like .../February-2026-Sports-Wagering-Data.xlsx"""
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12
        }
        combined = (xlsx_url + " " + page_url).lower()
        for month_name, month_num in months.items():
            if month_name in combined:
                year_match = re.search(rf'{month_name}[- _]*(\d{{4}})', combined)
                if year_match:
                    year = int(year_match.group(1))
                    last_day = calendar.monthrange(year, month_num)[1]
                    return date(year, month_num, last_day)
        return None

    def download_report(self, period_info: dict) -> Path:
        """Download MD Excel report."""
        url = period_info["download_url"]
        period_end = period_info["period_end"]
        filename = f"MD_{period_end.year}_{period_end.month:02d}.xlsx"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            return save_path

        download_file(url, save_path)
        self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
        return save_path

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse MD Excel report (retail + mobile sections)."""
        period_end = period_info["period_end"]
        source_url = period_info.get('download_url', period_info.get('url', None))

        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        # Find the data sheet (first one, usually "{Month} {Year} SW Data")
        data_sheet = xls.sheet_names[0]
        df = pd.read_excel(file_path, sheet_name=data_sheet, header=None)

        # Find the header row index for source_context (row with "Licensee"/"Handle")
        md_header_row = 0
        for _hi in range(min(20, len(df))):
            for _ci in range(min(10, df.shape[1])):
                _hv = df.iloc[_hi, _ci]
                if pd.notna(_hv) and "handle" in str(_hv).strip().lower():
                    md_header_row = _hi
                    break
            else:
                continue
            break

        all_rows = []
        current_channel = None

        for i in range(len(df)):
            val_a = df.iloc[i, 0] if df.shape[1] > 0 else None
            if pd.isna(val_a):
                continue

            label = str(val_a).strip()
            label_lower = label.lower()

            # Detect section headers
            if label_lower == "retail" or "retail" in label_lower and "total" not in label_lower:
                if "licensee" not in label_lower:
                    current_channel = "retail"
                    continue
            elif label_lower == "mobile" or "mobile" in label_lower and "total" not in label_lower:
                if "licensee" not in label_lower:
                    current_channel = "online"
                    continue
            elif "combined statewide" in label_lower:
                break  # Skip combined to avoid double counting

            if current_channel is None:
                continue

            # Skip headers and sub-headers
            if ("licensee" in label_lower or "handle" in label_lower or
                "month" in label_lower or not label):
                continue

            # Check col B for "Month" vs "FYTD" — only want monthly rows
            val_b = df.iloc[i, 1] if df.shape[1] > 1 else None
            if pd.notna(val_b):
                b_str = str(val_b).strip().lower()
                if "fytd" in b_str or "fiscal" in b_str:
                    continue  # Skip FYTD rows

            # Parse financial data
            handle = self._parse_money(df.iloc[i, 2] if df.shape[1] > 2 else None)
            payouts = self._parse_money(df.iloc[i, 3] if df.shape[1] > 3 else None)
            promo = self._parse_money(df.iloc[i, 5] if df.shape[1] > 5 else None)
            taxable_win = self._parse_money(df.iloc[i, 7] if df.shape[1] > 7 else None)
            tax = self._parse_money(df.iloc[i, 8] if df.shape[1] > 8 else None)

            if handle is None and taxable_win is None:
                continue

            # Skip total/subtotal rows
            if "total" in label_lower or "subtotal" in label_lower:
                continue

            # standard_ggr = handle - payouts (standardized cross-state metric)
            standard_ggr = None
            if handle is not None and payouts is not None:
                standard_ggr = handle - payouts

            # gross_revenue = state's reported "Taxable Win" (after promos)
            # This is the number MD publishes; standard_ggr is our handle - payouts metric
            gross_revenue = taxable_win

            # Capture raw cell values from this Excel row
            raw_cells = [str(v) for c in range(min(10, df.shape[1]))
                         if pd.notna(v := df.iloc[i, c]) and str(v).strip()]

            # Build source context for dashboard visual
            context_json = self.build_source_context(df, md_header_row, i)

            all_rows.append({
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": label,
                "channel": current_channel,
                "handle": handle,
                "gross_revenue": gross_revenue,
                "standard_ggr": standard_ggr,
                "promo_credits": promo,
                "net_revenue": taxable_win,
                "payouts": payouts,
                "tax_paid": tax,
                "source_file": file_path.name,
                "source_sheet": data_sheet,
                "source_row": i + 1,
                "source_url": source_url,
                "source_raw_line": ' | '.join(raw_cells),
                "source_context": context_json,
            })

        # Parse sport breakdown from "Bets By Sport" sheet (if present)
        sport_rows = self._parse_sport_sheet(xls, file_path, period_end, source_url)
        all_rows.extend(sport_rows)

        if not all_rows:
            return pd.DataFrame()

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        return result

    def _parse_sport_sheet(self, xls, file_path: Path, period_end: date, source_url: str = None) -> list[dict]:
        """Parse the 'Bets By Sport' sheet for sport-level breakdown.

        Sheet name varies: 'Bets By Sport', 'Bets By Sports', 'Bets by Sport',
        '{Month} {Year} Bets By Sport', etc. We match case-insensitively.

        Layout:
          Row N:   header (Total Wagered, % of Total, Total Payouts, Hold, Hold %)
          Row N+1: Golf        $6,041,135.62  1.17%  $5,582,499.47  $458,636.15  7.59%
          ...
          Row N+K: Total       $515,655,127   100%   ...
          (then FY-to-date section which we skip)

        We extract ONLY the monthly section (first set of rows before 'Total').
        """
        # Find the sport sheet
        sport_sheet = None
        for name in xls.sheet_names:
            if 'bets by sport' in name.lower():
                sport_sheet = name
                break

        if not sport_sheet:
            return []

        try:
            df = pd.read_excel(file_path, sheet_name=sport_sheet, header=None)
        except Exception:
            return []

        rows = []
        found_header = False
        past_first_total = False

        for i in range(len(df)):
            val_a = df.iloc[i, 0] if df.shape[1] > 0 else None

            # Look for header row
            if not found_header:
                val_c = str(df.iloc[i, 2]) if df.shape[1] > 2 and pd.notna(df.iloc[i, 2]) else ''
                if 'total wagered' in val_c.lower() or 'wagered' in val_c.lower():
                    found_header = True
                continue

            if pd.isna(val_a):
                continue

            sport_name = str(val_a).strip()
            sport_lower = sport_name.lower()

            # Skip empty, header-like, or FY section rows
            if not sport_name or 'fiscal' in sport_lower or sport_lower.startswith('20'):
                past_first_total = True
                continue

            # Stop after first "Total" row (monthly section ends)
            if sport_lower == 'total':
                past_first_total = True
                continue

            # Skip rows after the first Total (FY-to-date section)
            if past_first_total:
                # Check if this is another header for FY section
                val_c = str(df.iloc[i, 2]) if df.shape[1] > 2 and pd.notna(df.iloc[i, 2]) else ''
                if 'total wagered' in val_c.lower():
                    break  # Definitely in FY section now
                continue

            # Parse values: col 2=handle, col 4=payouts, col 5=GGR(hold)
            handle = self._parse_money(df.iloc[i, 2] if df.shape[1] > 2 else None)
            payouts = self._parse_money(df.iloc[i, 4] if df.shape[1] > 4 else None)
            ggr = self._parse_money(df.iloc[i, 5] if df.shape[1] > 5 else None)

            if handle is None and ggr is None:
                continue

            # Capture raw cell values from this Excel row
            raw_cells = [str(v) for c in range(min(8, df.shape[1]))
                         if pd.notna(v := df.iloc[i, c]) and str(v).strip()]

            rows.append({
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": "ALL",
                "channel": "combined",
                "handle": handle,
                "payouts": payouts,
                "gross_revenue": ggr,
                "standard_ggr": ggr,
                "sport_category": sport_name,
                "source_file": file_path.name,
                "source_sheet": sport_sheet,
                "source_row": i + 1,
                "source_url": source_url,
                "source_raw_line": ' | '.join(raw_cells),
            })

        if rows:
            self.logger.info(f"  Sports: {len(rows)} categories from '{sport_sheet}'")

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
    scraper = MDScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"MD SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Operators: {df['operator_standard'].nunique()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")

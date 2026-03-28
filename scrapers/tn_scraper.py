"""
Tennessee Sports Wagering Scraper
Source: tn.gov SWAC reports page
Format: CSV (Nov 2023+) + PDF (Nov 2020 - Oct 2023, and some months in 2025)
Launch: November 2020
Tax: 1.85% on HANDLE since July 2023; was 20% on AGI before that
Note: Aggregate data only — no operator breakdown; online only.
      Three format eras:
      - Era 1 (Nov 2020 - Jun 2023): Gross Wagers, Payouts, AGI, Privilege Tax (20% on AGI)
        Early months (Nov 2020 - ~mid 2021) use abbreviated "$211.3 Million" format.
      - Era 2 (Jul 2023 - Oct 2023): Gross Wagers, Adjustments, Gross Handle, Privilege Tax (1.85% on handle)
      - Era 3 (Nov 2023+): Same fields as Era 2, available in CSV (PDF also present)
"""

import sys
import re
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, fetch_with_retry

TN_REPORTS_URL = "https://www.tn.gov/swac/reports.html"
TN_BASE_URL = "https://www.tn.gov"

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


class TNScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("TN")

    def discover_periods(self) -> list[dict]:
        """Discover CSV and PDF download links from the TN SWAC reports page."""
        periods_by_key: dict[str, dict] = {}

        try:
            resp = fetch_with_retry(TN_REPORTS_URL)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            self.logger.error(f"Failed to fetch TN reports page: {e}")
            return []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            href_lower = href.lower()

            is_csv = ".csv" in href_lower
            is_pdf = ".pdf" in href_lower

            if not is_csv and not is_pdf:
                continue

            # Skip non-report files (audits, images)
            if "audit" in href_lower or ".jpg" in href_lower:
                continue

            full_url = href if href.startswith("http") else TN_BASE_URL + href
            period_end = self._extract_date(href, link.get_text(strip=True))
            if period_end is None:
                continue

            key = f"{period_end.year}-{period_end.month:02d}"

            if key not in periods_by_key:
                periods_by_key[key] = {
                    "period_end": period_end,
                    "period_type": "monthly",
                }

            # Prefer CSV over PDF
            if is_csv:
                periods_by_key[key]["csv_url"] = full_url
            elif is_pdf and "pdf_url" not in periods_by_key[key]:
                periods_by_key[key]["pdf_url"] = full_url

        periods = sorted(periods_by_key.values(), key=lambda p: p["period_end"])
        csv_count = sum(1 for p in periods if "csv_url" in p)
        pdf_count = sum(1 for p in periods if "pdf_url" in p)
        self.logger.info(f"  Found {len(periods)} months ({csv_count} CSV, {pdf_count} PDF)")
        return periods

    def _extract_date(self, url: str, text: str) -> date | None:
        """Extract month/year from URL path or link text."""
        combined = (url + " " + text).lower()

        # Try to find year in URL path like /2024/ or /2023/
        year = None
        year_match = re.search(r'/(\d{4})/', url)
        if year_match:
            year = int(year_match.group(1))

        # Find month name
        for month_name, month_num in MONTH_MAP.items():
            if month_name in combined:
                if year is None:
                    # Try year near month name
                    m = re.search(rf'{month_name}\s*(\d{{4}})', combined)
                    if m:
                        year = int(m.group(1))
                    else:
                        continue

                last_day = calendar.monthrange(year, month_num)[1]
                return date(year, month_num, last_day)

        return None

    def download_report(self, period_info: dict) -> Path:
        """Download TN report (prefer CSV, fall back to PDF)."""
        period_end = period_info["period_end"]
        prefix = f"TN_{period_end.year}_{period_end.month:02d}"

        # Try CSV first
        if "csv_url" in period_info:
            csv_path = self.raw_dir / f"{prefix}.csv"
            if not (csv_path.exists() and csv_path.stat().st_size > 50):
                resp = fetch_with_retry(period_info["csv_url"])
                with open(csv_path, "w") as f:
                    f.write(resp.text)
                self.logger.info(f"  Downloaded: {csv_path.name}")
            period_info["_file_type"] = "csv"
            return csv_path

        # Fall back to PDF
        if "pdf_url" in period_info:
            pdf_path = self.raw_dir / f"{prefix}.pdf"
            if not (pdf_path.exists() and pdf_path.stat().st_size > 1000):
                resp = fetch_with_retry(period_info["pdf_url"])
                with open(pdf_path, "wb") as f:
                    f.write(resp.content)
                self.logger.info(f"  Downloaded: {pdf_path.name}")
            period_info["_file_type"] = "pdf"
            return pdf_path

        raise FileNotFoundError(f"No CSV or PDF URL for TN {period_end}")

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse TN report from CSV or PDF."""
        period_end = period_info["period_end"]
        file_type = period_info.get("_file_type", "")

        if not file_type:
            file_type = "csv" if file_path.suffix == ".csv" else "pdf"

        if file_type == "csv":
            text = self._read_csv_as_text(file_path)
        else:
            text = self._read_pdf_as_text(file_path)

        if not text:
            return pd.DataFrame()

        row = self._parse_text(text, period_end)
        if row is None:
            return pd.DataFrame()

        # Source provenance fields
        source_url = period_info.get('download_url', period_info.get('url', None))
        if source_url is None:
            source_url = period_info.get('csv_url', period_info.get('pdf_url', None))
        row["source_file"] = file_path.name
        row["source_url"] = source_url
        row["source_raw_line"] = text.strip()
        if file_type == "csv":
            row["source_row"] = None
            row["source_sheet"] = None
            row["source_page"] = None
            # TN CSV is non-tabular text — no DataFrame context available
            row["source_context"] = None
        else:
            # PDF-parsed: no tabular context (skip per instructions)
            row["source_row"] = None
            row["source_page"] = 1
            row["source_table_index"] = 0
            row["source_sheet"] = None
            row["source_context"] = None

        result = pd.DataFrame([row])
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        return result

    def _read_csv_as_text(self, file_path: Path) -> str:
        """Read CSV as raw text (TN CSVs are non-tabular text reports)."""
        with open(file_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
        return content.replace("\xa0", " ")

    def _read_pdf_as_text(self, file_path: Path) -> str:
        """Extract text from PDF."""
        try:
            pdf = pdfplumber.open(file_path)
            parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
            pdf.close()
            return "\n".join(parts)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return ""

    def _parse_text(self, text: str, period_end: date) -> dict | None:
        """Parse TN report text (works for both CSV and PDF content).

        Handles three format eras:
        - Era 1 (pre-Jul 2023): Gross Wagers, Payouts, Adjustments, AGI, Tax (20% on AGI)
        - Era 2 (Jul-Oct 2023): Gross Wagers, Adjustments, Gross Handle, Tax (1.85% on handle)
        - Era 3 (Nov 2023+): Same as Era 2
        """
        lines = text.splitlines()

        gross_wagers = None
        payouts = None
        adjustments = None
        gross_handle = None
        agi = None
        privilege_tax = None

        for line in lines:
            clean = line.strip()
            if not clean:
                continue
            lower = clean.lower()

            if "gross wager" in lower:
                gross_wagers = self._extract_dollar(clean)
            elif "payout" in lower:
                payouts = self._extract_dollar(clean)
            elif "adjustment" in lower:
                adjustments = self._extract_dollar(clean)
            elif "gross handle" in lower:
                gross_handle = self._extract_dollar(clean)
            elif "adjusted gross income" in lower or "adjusted gross rev" in lower:
                agi = self._extract_dollar(clean)
            elif "privilege tax" in lower:
                privilege_tax = self._extract_dollar(clean)

        # Determine handle
        handle = gross_handle or gross_wagers
        if handle is None:
            return None

        row = {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "ALL",
            "channel": "online",
            "handle": handle,
        }

        if privilege_tax is not None:
            row["tax_paid"] = privilege_tax

        # Era 1: has payouts and AGI (pre-Jul 2023)
        if payouts is not None:
            row["payouts"] = payouts
            row["gross_revenue"] = handle - payouts
            row["standard_ggr"] = handle - payouts
            if agi is not None:
                row["net_revenue"] = agi
                # promo/adjustments = GGR - AGI
                promo = (handle - payouts) - agi
                if promo > 0:
                    row["promo_credits"] = promo
        elif agi is not None:
            # Has AGI but no separate payouts line
            row["net_revenue"] = agi
        else:
            # Era 2+ (Jul 2023+): TN stopped reporting payouts/GGR after switching
            # to handle-based tax. Estimate GGR using 10% hold assumption (industry
            # average for TN market based on Era 1 actuals which averaged ~9.9% hold).
            # These are ESTIMATED values, not reported by the state.
            estimated_ggr = handle * 0.10
            row["gross_revenue"] = estimated_ggr
            row["standard_ggr"] = estimated_ggr
            row["payouts"] = handle - estimated_ggr

        return row

    def _extract_dollar(self, line: str) -> float | None:
        """Extract dollar amount from various TN report formats.

        Handles:
        - Standard: $538,002,852
        - Abbreviated: $211.3 Million
        - CSV quoted: , " $538,002,852 "
        - PDF space artifacts: $ 4 40,636,125 (space inside number)
        """
        # Try abbreviated format first: "$211.3 Million" / "$ 4.3 Million"
        m = re.search(r'\$\s*([\d.]+)\s*[Mm]illion', line)
        if m:
            try:
                return float(m.group(1)) * 1_000_000
            except ValueError:
                pass

        # Standard dollar amount: $538,002,852
        # Also handles PDF space artifacts like "$ 4 40,636,125" by allowing
        # spaces within the digit/comma sequence after the $ sign.
        m = re.search(r'\$\s*([\d,\s]+(?:\.\d+)?)', line)
        if m:
            val_str = m.group(1).replace(',', '').replace(' ', '')
            try:
                return float(val_str)
            except ValueError:
                pass

        # CSV comma-separated fields
        parts = line.split(",")
        for part in parts:
            cleaned = part.strip().strip('"').strip().replace('$', '').replace(',', '').strip()
            if cleaned and re.match(r'^[\d.]+$', cleaned):
                try:
                    return float(cleaned)
                except ValueError:
                    continue

        return None


if __name__ == "__main__":
    scraper = TNScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"TN SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        monthly = df[df['period_type'] == 'monthly']
        if not monthly.empty:
            avg = monthly['handle'].mean()
            print(f"Avg monthly handle: ${avg/100:,.0f}")

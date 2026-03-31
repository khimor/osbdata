"""
South Dakota Sports Betting Revenue Scraper
Source: SD Department of Revenue monthly gaming statistics PDFs
Format: PDF (monthly gaming stats with sports wagering section)
Launch: November 2021
Tax: 9% on GGR
Channel: Retail only (Deadwood casinos), aggregate data
Very small market (~$12M annual handle)
Note: URL slugs contain random hashes (/media/{hash}/), so we must scrape
      the index page at dor.sd.gov/businesses/gaming/ to find PDF links.
"""

import sys
import re
import calendar
from pathlib import Path
from datetime import date, datetime
from urllib.parse import urljoin

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, clean_currency

SD_INDEX_URL = "https://dor.sd.gov/businesses/gaming/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}

# Minimum date for monthly PDFs (annual compilations cover earlier periods)
SD_MONTHLY_START = date(2023, 1, 1)

# Regex for money values: positive $X,XXX.XX and negative ($X,XXX.XX)
_MONEY_RE = re.compile(r'\(\$[\d,]+\.?\d*\)|\$[\d,]+\.?\d*')


class SDScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("SD")

    # ------------------------------------------------------------------
    # discover_periods
    # ------------------------------------------------------------------
    def discover_periods(self) -> list[dict]:
        """
        Scrape the SD DOR gaming page to find monthly gaming statistics PDFs.
        URLs contain random hashes so we must discover them from the index page.
        Focuses on individual monthly PDFs from Jan 2023 onward.
        """
        periods = []
        seen = set()

        try:
            resp = requests.get(SD_INDEX_URL, headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
            }, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True)

                if ".pdf" not in href.lower():
                    continue

                # Skip non-gaming-stats PDFs: racing, applications, annual reports, etc.
                combined_lower = (href + " " + text).lower()
                if any(skip in combined_lower for skip in [
                    "racing", "pari-mutuel", "simulcast", "horse",
                    "license", "application", "schedule",
                    "exclusion", "minutes", "catalog",
                    "annual-report", "annual report",
                    "1989-2013",
                ]):
                    continue

                # Skip the annual compilation PDFs (gaming-stats-2022-1.pdf, etc.)
                # These cover full years but we prefer the individual monthly files
                if re.search(r'gaming-stats-20\d{2}', href.lower()):
                    continue

                # Must be a gaming stats / monthly statistics PDF
                # The link text or URL should reference a month name
                period_end = self._extract_date(href, text)
                if period_end is None:
                    continue

                # Only take Jan 2023+ (monthly PDFs exist from this point)
                if period_end < SD_MONTHLY_START:
                    continue

                if period_end in seen:
                    continue
                seen.add(period_end)

                full_url = urljoin(SD_INDEX_URL, href)
                periods.append({
                    "download_url": full_url,
                    "period_end": period_end,
                    "period_type": "monthly",
                })

        except Exception as e:
            self.logger.warning(f"Could not scrape SD index page: {e}")

        self.logger.info(f"  Discovered {len(periods)} SD periods")
        return sorted(periods, key=lambda p: p["period_end"])

    # ------------------------------------------------------------------
    # _extract_date
    # ------------------------------------------------------------------
    def _extract_date(self, href: str, text: str) -> date | None:
        """Extract a month/year from a PDF link URL and/or link text."""
        combined = (href + " " + text).lower()

        # Try matching month names — prefer longer names first to avoid
        # "jan" matching inside "january"
        sorted_months = sorted(MONTH_NAMES.items(), key=lambda x: -len(x[0]))
        for mname, mnum in sorted_months:
            if mname in combined:
                # Find a 4-digit year
                year_match = re.search(r'(20\d{2})', combined)
                if year_match:
                    year = int(year_match.group(1))
                    if 2021 <= year <= 2030:
                        last_day = calendar.monthrange(year, mnum)[1]
                        return date(year, mnum, last_day)
                # Try 2-digit year (e.g., "feb-24" in URL "feb-24-gaming-stats")
                # But avoid matching random 2-digit numbers in hashes
                year2_match = re.search(
                    rf'{mname}[\s\-_]+(\d{{2}})(?:\D|$)', combined
                )
                if year2_match:
                    y2 = int(year2_match.group(1))
                    if 21 <= y2 <= 30:
                        year = 2000 + y2
                        last_day = calendar.monthrange(year, mnum)[1]
                        return date(year, mnum, last_day)

        return None

    # ------------------------------------------------------------------
    # download_report
    # ------------------------------------------------------------------
    def download_report(self, period_info: dict) -> Path:
        """Download SD monthly gaming stats PDF."""
        url = period_info["download_url"]
        period_end = period_info["period_end"]
        filename = f"SD_{period_end.year}_{period_end.month:02d}.pdf"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            return save_path

        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=60)

        if resp.status_code != 200:
            raise FileNotFoundError(
                f"SD PDF not found: {url} (status {resp.status_code})"
            )

        with open(save_path, "wb") as f:
            f.write(resp.content)

        self.logger.info(
            f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)"
        )
        return save_path

    # ------------------------------------------------------------------
    # parse_report
    # ------------------------------------------------------------------
    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """
        Parse SD monthly gaming stats PDF.

        Page 1: Summary with "Sports Wagering" totals line.
        Page 2: "Sports Wagering Detail" with per-sport breakdown and Totals.

        We extract:
         - One aggregate row (sport_category=None) from the Totals line
         - Per-sport rows from the detail table
        All returned as channel="retail".
        """
        period_end = period_info["period_end"]

        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        # Extract text from all pages
        page_texts = []
        for page in pdf.pages:
            page_texts.append(page.extract_text() or "")

        # If text extraction yields nothing, try OCR
        all_text = "\n".join(page_texts)
        if len(all_text.strip()) < 100:
            page_texts = self._ocr_pages(pdf)
            all_text = "\n".join(page_texts)

        pdf.close()

        # Strategy 1: Parse "Sports Wagering Detail" page for per-sport + totals
        rows = self._parse_detail_page(all_text, period_end)

        # Strategy 2: Fall back to page 1 summary if detail page failed
        if not rows:
            agg = self._parse_summary_totals(all_text, period_end)
            if agg:
                rows = [agg]

        if not rows:
            self.logger.warning(
                f"No sports wagering data found in {file_path.name}"
            )
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Source provenance fields
        df["source_file"] = file_path.name
        df["source_url"] = period_info.get('download_url', period_info.get('url', None))
        # Page-level tracking not available (text from all pages is concatenated)
        df["source_page"] = None

        df["period_end"] = pd.to_datetime(df["period_end"])
        df["period_start"] = df["period_end"].apply(lambda d: d.replace(day=1))
        return df

    # ------------------------------------------------------------------
    # _ocr_pages — fallback for image-based PDFs
    # ------------------------------------------------------------------
    def _ocr_pages(self, pdf) -> list[str]:
        """Use OCR to extract text from image-based PDF pages.

        Uses PSM 4 (assume a single column of text) which preserves the
        tabular row alignment much better than the default PSM 3.
        """
        texts = []
        try:
            import pytesseract
        except ImportError:
            self.logger.warning("pytesseract not installed — cannot OCR")
            return [""] * len(pdf.pages)

        for page in pdf.pages:
            try:
                img = page.to_image(resolution=300)
                pil_img = img.original
                text = pytesseract.image_to_string(pil_img, config="--psm 4")
                texts.append(text)
            except Exception as e:
                self.logger.warning(f"OCR failed on page: {e}")
                texts.append("")

        self.logger.info("  Used OCR fallback for image-based PDF")
        return texts

    # ------------------------------------------------------------------
    # _parse_detail_page — parse Sports Wagering Detail (page 2)
    # ------------------------------------------------------------------
    def _parse_detail_page(self, text: str, period_end: date) -> list[dict]:
        """
        Parse the "Sports Wagering Detail" section.

        Format:
            Sports Wagering Detail
            <Month Year>
            Sporting Event    Handle    Statistical Win    Avg. Payout %
            BOXING            $600.00   $588.45           1.92%
            ...
            Totals            $1,079,319.95  $36,852.94   96.59%

        Returns a list of dicts: per-sport rows + one aggregate row (from Totals).
        """
        lines = text.splitlines()
        in_detail = False
        sport_rows = []
        totals_row = None

        for line in lines:
            line_stripped = line.strip()
            line_lower = line_stripped.lower()

            # Detect start of detail section
            if "sports wagering detail" in line_lower:
                in_detail = True
                continue

            if not in_detail:
                continue

            # Skip header lines and date lines
            if not line_stripped:
                continue
            if any(h in line_lower for h in [
                "sporting event", "handle", "avg. payout", "avg. payer",
                "avg payout",
            ]):
                continue
            # Skip month/year header like "January 2026"
            if re.match(r'^[A-Za-z]+\s+\d{4}$', line_stripped):
                continue

            # Parse the Totals line (handle OCR misspellings like "Totais")
            if line_lower.startswith("total") or re.match(r'^totai', line_lower):
                money_strs = _MONEY_RE.findall(line)
                money_vals = [clean_currency(s) for s in money_strs]
                money_vals = [v for v in money_vals if v is not None]

                if len(money_vals) >= 2:
                    handle = money_vals[0]
                    stat_win = money_vals[1]
                    totals_row = self._build_row(
                        period_end, handle, stat_win, sport_category=None,
                        source_raw_line=line_stripped,
                    )
                elif len(money_vals) == 1:
                    totals_row = self._build_row(
                        period_end, money_vals[0], None, sport_category=None,
                        source_raw_line=line_stripped,
                    )
                # Done with the detail section after Totals
                break

            # Parse individual sport lines
            # Format: "SPORT_NAME  $handle  $stat_win  pct%"
            # or "SPORT_NAME  $handle  ($stat_win)  pct%"
            # Some lines have $0.00 $0.00 #DIV/0!
            money_strs = _MONEY_RE.findall(line)
            if len(money_strs) >= 2:
                # Extract sport name: everything before the first $ or (
                sport_match = re.match(r'^(.+?)(?:\$|\()', line_stripped)
                if sport_match:
                    sport_name = sport_match.group(1).strip()
                else:
                    sport_name = line_stripped.split()[0] if line_stripped.split() else "Unknown"

                money_vals = [clean_currency(s) for s in money_strs]
                money_vals = [v for v in money_vals if v is not None]

                if len(money_vals) >= 2:
                    handle = money_vals[0]
                    stat_win = money_vals[1]
                    # Skip zero-handle sports (no activity)
                    if handle == 0.0 and stat_win == 0.0:
                        continue
                    sport_rows.append(self._build_row(
                        period_end, handle, stat_win, sport_category=sport_name,
                        source_raw_line=line_stripped,
                    ))

        # Aggregate sport rows that normalize to the same sport category.
        # E.g. NBA + WNBA both -> "basketball", NCAA FB + NFL both -> different,
        # but NCAA Hockey -> "hockey" same as NHL -> "hockey".
        # Import normalize_sport to pre-aggregate before the base scraper dedup.
        from scrapers.operator_mapping import normalize_sport

        # Map normalized category -> a raw name the SPORT_MAP will recognize.
        # We must return raw names (SPORT_MAP keys) so that base_scraper's
        # normalize_sport call produces the correct normalized value.
        _NORM_TO_RAW = {
            "football": "Football",
            "football_college": "College Football",
            "basketball": "Basketball",
            "basketball_college": "College Basketball",
            "baseball": "Baseball",
            "hockey": "Hockey",
            "soccer": "Soccer",
            "tennis": "Tennis",
            "golf": "Golf",
            "boxing": "Boxing",
            "mma": "MMA",
            "motorsports": "Motor Racing",
            "other": "Other",
            "cricket": "Cricket",
            "rugby": "Rugby",
            "esports": "Esports",
            "parlay": "Parlay",
            "table_tennis": "Table Tennis",
            "lacrosse": "Lacrosse",
        }

        aggregated = {}
        for row in sport_rows:
            raw_sport = row.get("sport_category", "")
            norm = normalize_sport(raw_sport) or "other"
            if norm not in aggregated:
                aggregated[norm] = {
                    "handle": 0.0,
                    "gross_revenue": 0.0,
                    "source_raw_lines": [],
                }
            aggregated[norm]["handle"] += row.get("handle", 0.0) or 0.0
            if row.get("gross_revenue") is not None:
                aggregated[norm]["gross_revenue"] += row["gross_revenue"]
            if row.get("source_raw_line"):
                aggregated[norm]["source_raw_lines"].append(row["source_raw_line"])

        result = []
        for norm_sport, vals in aggregated.items():
            # Use a raw sport name that SPORT_MAP will correctly normalize
            raw_name = _NORM_TO_RAW.get(norm_sport, norm_sport)
            # Join multiple source lines if sport categories were aggregated
            agg_source = ' | '.join(vals["source_raw_lines"]) if vals["source_raw_lines"] else None
            result.append(self._build_row(
                period_end,
                vals["handle"],
                vals["gross_revenue"],
                sport_category=raw_name,
                source_raw_line=agg_source,
            ))

        if totals_row:
            result.append(totals_row)
        elif result:
            # If we got sport rows but no totals line, compute aggregate
            total_handle = sum(r["handle"] for r in result if r.get("handle"))
            total_ggr = sum(
                r["gross_revenue"] for r in result
                if r.get("gross_revenue") is not None
            )
            result.append(self._build_row(
                period_end, total_handle, total_ggr, sport_category=None
            ))

        return result

    # ------------------------------------------------------------------
    # _parse_summary_totals — fallback parse from page 1 summary
    # ------------------------------------------------------------------
    def _parse_summary_totals(self, text: str, period_end: date) -> dict | None:
        """
        Fallback: extract from the page 1 summary "Sports Wagering" section.

        Section looks like:
            Sports Wagering
            Number of Casinos Reporting
            Revenue Handle Statistical Win Avg. Payout %
            Totals  7  $1,001,806.31  $145,591.19  85.47%

        Must stop before "Handle Comparison" (which has YTD data).
        """
        lines = text.splitlines()
        in_sports_summary = False

        for line in lines:
            line_lower = line.lower().strip()

            # Enter the sports wagering summary section (but NOT the detail page)
            if ("sports wagering" in line_lower
                    and "detail" not in line_lower
                    and "comparison" not in line_lower):
                in_sports_summary = True
                continue

            # Exit before Handle Comparison or other sections
            if in_sports_summary:
                if any(kw in line_lower for kw in [
                    "handle comparison", "tax calculation",
                    "slot machine", "table game", "total gaming",
                    "sports wagering detail",
                ]):
                    in_sports_summary = False
                    continue

                if line_lower.startswith("totals"):
                    money_strs = _MONEY_RE.findall(line)
                    money_vals = [clean_currency(s) for s in money_strs]
                    money_vals = [v for v in money_vals if v is not None]

                    if len(money_vals) >= 2:
                        # Page 1 Totals: first value after count is handle,
                        # second is statistical win
                        return self._build_row(
                            period_end, money_vals[0], money_vals[1],
                            sport_category=None,
                            source_raw_line=line.strip(),
                        )
                    elif len(money_vals) == 1:
                        return self._build_row(
                            period_end, money_vals[0], None,
                            sport_category=None,
                            source_raw_line=line.strip(),
                        )

        return None

    # ------------------------------------------------------------------
    # _build_row — construct a standardized data row
    # ------------------------------------------------------------------
    def _build_row(
        self,
        period_end: date,
        handle: float | None,
        stat_win: float | None,
        sport_category: str | None,
        source_raw_line: str | None = None,
    ) -> dict:
        """
        Build a data row dict.

        - handle: gross wagers (dollars)
        - stat_win: Statistical Win = GGR = handle - payouts (can be negative)
        - payouts: handle - stat_win
        - tax_paid: 9% of stat_win if positive, else 0
        - sport_category: raw sport name from PDF, or None for aggregate
        """
        row = {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "Deadwood Casinos",
            "channel": "retail",
            "handle": handle,
        }

        if stat_win is not None:
            row["gross_revenue"] = stat_win
            row["standard_ggr"] = stat_win

            if handle is not None:
                row["payouts"] = handle - stat_win

            # Tax: 9% of Statistical Win if positive
            if stat_win > 0:
                row["tax_paid"] = round(stat_win * 0.09, 2)
            else:
                row["tax_paid"] = 0.0

        if sport_category is not None:
            row["sport_category"] = sport_category

        if source_raw_line is not None:
            row["source_raw_line"] = source_raw_line

        return row


if __name__ == "__main__":
    scraper = SDScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"SD SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        if 'sport_category' in df.columns:
            agg = df[df['sport_category'].isna()]
            sport = df[df['sport_category'].notna()]
            print(f"Aggregate rows: {len(agg)}")
            print(f"Sport breakdown rows: {len(sport)}")
            print(f"Sports: {sorted(sport['sport_category'].dropna().unique())}")
        unique_months = df[df['period_type'] == 'monthly']['period_end'].nunique()
        print(f"Unique months: {unique_months}")

"""
Nevada Sports Pool Revenue Scraper
Source: Nevada Gaming Control Board monthly GRI (Gaming Revenue Information) reports
Format: PDF (~48 pages per report), sports data on specific pages
Launch: 1949 (oldest legal sports betting market in the US)
Tax: 6.75% on gross gaming revenue
Note: No operator breakdown — aggregate statewide data.
      Sports pool data broken down by sport category (Football, Basketball, Baseball, etc.)
      Win amounts reported in THOUSANDS — must multiply by 1000.
      Handle derived from Win Amount / Win Percent (no Write column in PDFs).
      Includes both race book and sports pool; we extract sports pool only.
      Mobile/online sports data reported separately starting ~2020.
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

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

GRI_INDEX_URL = "https://www.gaming.nv.gov/about-us/gaming-revenue-information-gri/"

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

MONTH_ABBRS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Sport labels to look for in the footnote detail section.
# Keys are lowercase prefixes we match against; values are the sport_category
# name we emit (which normalize_sport() will further standardize).
SPORT_LABELS = {
    "football": "Football",
    "basketball": "Basketball",
    "baseball": "Baseball",
    "sports parlay cards": "Sports Parlay Cards",
    "parlay cards": "Sports Parlay Cards",
    "hockey": "Hockey",
    "other": "Other",
}

NV_TAX_RATE = 0.0675
NV_START_YEAR = 2010
NV_START_MONTH = 1


class NVScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("NV")

    # ------------------------------------------------------------------
    # discover_periods: scrape the GRI index page for all PDF links
    # ------------------------------------------------------------------
    def discover_periods(self) -> list[dict]:
        """
        Scrape the NGCB GRI index page to discover all available PDF URLs.
        Returns one period dict per month from NV_START_YEAR/MONTH to present.
        """
        pdf_urls = self._scrape_gri_links()

        periods = []
        today = date.today()
        year, month = NV_START_YEAR, NV_START_MONTH

        while True:
            last_day = calendar.monthrange(year, month)[1]
            period_end = date(year, month, last_day)
            if period_end > today:
                break

            period = {
                "period_end": period_end,
                "period_type": "monthly",
                "year": year,
                "month": month,
            }

            key = (year, month)
            if key in pdf_urls:
                period["download_url"] = pdf_urls[key]

            periods.append(period)

            month += 1
            if month > 12:
                month = 1
                year += 1

        self.logger.info(
            f"  Discovered {len(periods)} monthly periods "
            f"({len(pdf_urls)} with scraped URLs)"
        )
        return periods

    def _scrape_gri_links(self) -> dict:
        """
        Scrape the GRI index page and extract all PDF links with their
        (year, month) mapping. Returns {(year, month): full_url}.
        """
        links = {}
        try:
            resp = requests.get(GRI_INDEX_URL, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning(f"  Could not fetch GRI index page: {e}")
            return links

        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ".pdf" not in href.lower():
                continue

            # Combine link text and href for date extraction
            link_text = a.get_text(strip=True)
            combined = (link_text + " " + href).lower()

            # Only keep GRI / revenue report links
            if not any(kw in combined for kw in ["gri", "revenue", "monthly"]):
                continue

            period = self._extract_date_from_link(href, link_text)
            if period is None:
                continue

            year, month = period
            if year < NV_START_YEAR:
                continue

            full_url = href if href.startswith("http") else f"https://www.gaming.nv.gov{href}"
            links[(year, month)] = full_url

        self.logger.info(f"  Scraped {len(links)} PDF links from GRI index page")
        return links

    def _extract_date_from_link(self, href: str, text: str) -> tuple | None:
        """
        Extract (year, month) from a PDF link's href and/or text.
        Handles all known NGCB URL naming patterns.
        """
        combined = (text + " " + href).lower()

        # Pattern 1: "2025Jan" or "2025oct" in URL
        m = re.search(r'(\d{4})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', combined)
        if m:
            year = int(m.group(1))
            month = MONTH_ABBRS.get(m.group(2))
            if month and 2000 <= year <= 2030:
                return (year, month)

        # Pattern 2: "january-2026" or "august-2025" (full month name + year)
        for name, num in MONTH_NAMES.items():
            if name in combined:
                ym = re.search(r'(\d{4})', combined)
                if ym:
                    year = int(ym.group(1))
                    if 2000 <= year <= 2030:
                        return (year, num)

        # Pattern 3: link text like "GRI Jan 2026" or "Jan 2026"
        m2 = re.search(
            r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+(\d{4})',
            combined,
        )
        if m2:
            month = MONTH_ABBRS.get(m2.group(1)[:3])
            year = int(m2.group(2))
            if month and 2000 <= year <= 2030:
                return (year, month)

        return None

    # ------------------------------------------------------------------
    # download_report: fetch PDF to data/raw/NV/
    # ------------------------------------------------------------------
    def download_report(self, period_info: dict) -> Path:
        """Download NV GRI PDF report."""
        year = period_info["year"]
        month = period_info["month"]
        filename = f"NV_{year}_{month:02d}_gri.pdf"
        save_path = self.raw_dir / filename

        if save_path.exists() and save_path.stat().st_size > 10000:
            return save_path

        # Build candidate URLs in priority order
        month_name_full = calendar.month_name[month].lower()
        month_abbr_title = calendar.month_abbr[month]  # "Jan", "Feb"
        month_abbr_lower = month_abbr_title.lower()     # "jan", "feb"
        base = "https://www.gaming.nv.gov"

        candidates = []

        # Scraped URL from the index page — highest priority
        if "download_url" in period_info:
            candidates.append(period_info["download_url"])

        # siteassets patterns (2025 mid+, Nov/Dec 2025)
        candidates.extend([
            f"{base}/siteassets/content/about/gaming-revenue/{year}{month_abbr_title}-gri.pdf",
            f"{base}/siteassets/content/about/gaming-revenue/{year}{month_abbr_lower}-gri.pdf",
            f"{base}/siteassets/content/about/gaming-revenue/{month_name_full}-{year}-monthly-revenue-report.pdf",
            f"{base}/siteassets/content/about/gaming-revenue/{year}{month_abbr_title.capitalize()}-gri.pdf",
            f"{base}/siteassets/content/about/gaming-revenue/monthly-revenue-report---{month_name_full}-{year}.pdf",
        ])

        # contentassets pattern (Aug/Sep 2025)
        candidates.append(
            f"{base}/contentassets/a7958398526e4e309d248ea35a2a20dd/{month_name_full}-{year}-monthly-revenue-report.pdf"
        )

        # Legacy uploadedFiles pattern (2004-2025 early)
        candidates.extend([
            f"{base}/uploadedFiles/gamingnvgov/content/about/gaming-revenue/{year}{month_abbr_title}-gri.pdf",
            f"{base}/uploadedFiles/gamingnvgov/content/about/gaming-revenue/{year}{month_abbr_lower}-gri.pdf",
        ])

        # De-duplicate while preserving order
        seen = set()
        unique_candidates = []
        for url in candidates:
            url_lower = url.lower()
            if url_lower not in seen:
                seen.add(url_lower)
                unique_candidates.append(url)

        for url in unique_candidates:
            try:
                resp = requests.get(url, headers=HEADERS, timeout=60, allow_redirects=True)
                if resp.status_code == 200 and (
                    "pdf" in resp.headers.get("Content-Type", "").lower()
                    or resp.content[:4] == b'%PDF'
                ):
                    with open(save_path, "wb") as f:
                        f.write(resp.content)
                    if save_path.stat().st_size > 10000:
                        self.logger.info(
                            f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)"
                        )
                        return save_path
                    save_path.unlink(missing_ok=True)
            except Exception:
                continue

        raise FileNotFoundError(
            f"NV GRI report not found for {year}-{month:02d}. "
            f"Try downloading manually from {GRI_INDEX_URL}"
        )

    # ------------------------------------------------------------------
    # parse_report: extract sports pool data from GRI PDF
    # ------------------------------------------------------------------
    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """
        Parse NV GRI PDF to extract sports pool data.

        The GRI is a ~48 page report. The "Statewide - All Nonrestricted"
        summary page contains:
        1. A summary table with a "Sports Pool (2)" line — aggregate Win
           Amount and Win Percent for the current month.
        2. A footnote detail section at the bottom with per-sport breakdowns:
           "(2) Sports - Football", "Sports - Basketball", etc.
        3. A "*** Sports - Mobile" line showing mobile/online Win Amount.

        All Win amounts are in THOUSANDS — we multiply by 1000.
        Handle is derived: handle = win_amount / (win_percent / 100).
        """
        period_end = period_info["period_end"]
        source_url = period_info.get('download_url', period_info.get('url', None))

        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        # Find the Statewide - All Nonrestricted summary page
        page_text, source_page = self._find_statewide_page(pdf)
        pdf.close()

        if not page_text:
            self.logger.warning(f"  No statewide summary page found in {file_path.name}")
            return pd.DataFrame()

        lines = page_text.splitlines()

        # Phase 1: Parse the aggregate "Sports Pool (2)" summary line
        agg = self._extract_sports_pool_summary(lines)

        # Phase 2: Parse per-sport detail from the footnote section
        sport_rows = self._extract_sport_details(lines)

        # Phase 3: Parse the "*** Sports - Mobile" line
        mobile = self._extract_mobile_line(lines)

        # Build output rows
        all_rows = []

        # Mobile / online row
        if mobile:
            row = self._build_row(
                period_end, "online", None,
                mobile["ggr"], mobile["handle"],
            )
            row["source_raw_line"] = mobile.get("source_raw_line")
            all_rows.append(row)

        # Aggregate Sports Pool row (always include as the authoritative total)
        if agg:
            row = self._build_row(
                period_end, "combined", None,
                agg["ggr"], agg["handle"],
            )
            row["source_raw_line"] = agg.get("source_raw_line")
            all_rows.append(row)

        # Per-sport detail rows
        if sport_rows:
            for sr in sport_rows:
                row = self._build_row(
                    period_end, "combined", sr["sport"],
                    sr["ggr"], sr["handle"],
                )
                row["source_raw_line"] = sr.get("source_raw_line")
                all_rows.append(row)

        if not all_rows:
            self.logger.warning(f"  No sports data found in {file_path.name}")
            return pd.DataFrame()

        # Add source provenance fields
        for row in all_rows:
            row["source_file"] = file_path.name
            row["source_page"] = source_page
            row["source_url"] = source_url

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        return result

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------
    def _find_statewide_page(self, pdf) -> tuple[str | None, int | None]:
        """
        Find the Statewide - All Nonrestricted summary page in the PDF.
        Searches the first 10 pages. Accepts both old-format (pipe-delimited,
        uppercase headers) and new-format (clean text, title-case headers).

        Returns (page_text, 1-indexed page number) or (None, None).
        """
        best = None
        best_score = 0
        best_page_num = None

        for page in pdf.pages[:10]:
            text = page.extract_text() or ""
            if not text.strip():
                continue
            upper = text.upper()

            score = 0
            if "STATEWIDE" in upper:
                score += 2
            if "NONRESTRICTED" in upper:
                score += 2
            if "SPORTS POOL" in upper:
                score += 3
            if "FOOTBALL" in upper:
                score += 1
            if "BASKETBALL" in upper:
                score += 1

            # Must at least have Sports Pool to be useful
            if "SPORTS POOL" not in upper:
                continue

            if score > best_score:
                best_score = score
                best = text
                best_page_num = page.page_number  # 1-indexed

        return best, best_page_num

    def _extract_sports_pool_summary(self, lines: list[str]) -> dict | None:
        """
        Extract aggregate GGR and handle from the 'Sports Pool (2)' summary line.

        Format examples:
          Modern: "Sports Pool (2) 182 180 64,505 (11.14) 8.11 185 181 ..."
          Old:    "SPORTS POOL (2) | 185 185 15,342 93.23 5.22 | 185 182 ..."

        Columns: #Loc #Units WinAmount [%Chg] WinPercent [then 3-month, 12-month groups]
        """
        for line in lines:
            cleaned = line.replace("|", " ").strip()
            upper = cleaned.upper()
            # Match "Sports Pool (2)" but NOT the detail section header
            if re.match(r'^SPORTS\s+POOL\s*\(\d\)', upper):
                result = self._parse_summary_numbers(cleaned)
                if result:
                    result["source_raw_line"] = line.strip()
                return result
        return None

    def _parse_summary_numbers(self, line: str) -> dict | None:
        """
        Given a 'Sports Pool (2) ...' line, extract Current Month win amount
        and win percent.
        """
        # Strip the label prefix
        cleaned = re.sub(
            r'^.*?SPORTS\s+POOL\s*\(\d\)\s*',
            '', line, flags=re.IGNORECASE,
        ).strip()

        nums = self._extract_numbers(cleaned)
        if len(nums) < 3:
            return None

        # nums[0] = #Loc, nums[1] = #Units, nums[2] = WinAmount
        ggr = self._thousands_to_dollars(nums[2])

        # Determine WinPercent position (4-number or 5-number group)
        win_pct = self._find_win_pct(nums, start=3)
        handle = self._derive_handle(ggr, win_pct)

        return {"ggr": ggr, "handle": handle}

    def _extract_sport_details(self, lines: list[str]) -> list[dict]:
        """
        Extract per-sport detail rows from the footnote section.

        The detail section is identified by lines matching:
          New format: "(2) Sports - Football  180 180  28,107  (27.34)  7.71 ..."
          Old format: "FOOTBALL | 184 184 4,158 325.86 2.48 | ..."

        We enter "detail mode" when we see either "(2)SPORTS POOL DETAIL",
        "(2) Sports -", or the "*** Sports - Mobile" line (which sits
        adjacent to the detail rows).
        """
        results = []
        in_detail = False

        for line in lines:
            cleaned = line.replace("|", " ").strip()
            upper = cleaned.upper()

            # Detect entry into the detail section
            if "SPORTS POOL DETAIL" in upper:
                in_detail = True
                continue
            if re.match(r'^\(1\)\s*RACE', upper):
                in_detail = True
                continue
            if re.match(r'^\(\d\)\s*SPORTS\s*[-–]', upper):
                in_detail = True
                # Fall through to parse this line
            if "SPORTS - MOBILE" in upper or "SPORTS-MOBILE" in upper:
                in_detail = True
                continue  # Mobile handled separately

            if not in_detail:
                continue

            # Stop conditions
            if upper.startswith("NOTE:") or upper.startswith("COLUMNS MAY"):
                break
            if upper.startswith("AS OF:") or upper.startswith("PAGE "):
                break
            if re.match(r'^-{20,}$', cleaned):
                break

            # Try to match a sport category
            sport_label = self._match_sport_label(cleaned)
            if sport_label is None:
                continue

            parsed = self._parse_detail_numbers(cleaned, sport_label)
            if parsed:
                results.append({
                    "sport": sport_label,
                    "ggr": parsed["ggr"],
                    "handle": parsed["handle"],
                    "source_raw_line": line.strip(),
                })

        return results

    def _match_sport_label(self, line: str) -> str | None:
        """
        Match a line to one of the known sport categories.
        Returns the standard sport label or None.
        """
        # Strip footnote prefix like "(2) " or "(7) "
        stripped = re.sub(r'^\(\d\)\s*', '', line.strip())
        # Strip "Sports - " or "Sports " prefix
        stripped = re.sub(r'^[Ss]ports\s*[-–]\s*', '', stripped).strip()
        stripped_lower = stripped.lower()

        for key, label in SPORT_LABELS.items():
            if stripped_lower.startswith(key):
                return label
        return None

    def _parse_detail_numbers(self, line: str, sport_label: str) -> dict | None:
        """
        Parse a sport detail line to extract Current Month win amount and win percent.

        Format: "(2) Sports - Football  180 180  28,107  (27.34)  7.71  184 183 ..."
        or old: "FOOTBALL | 184 184 4,158 325.86 2.48 | ..."
        or:     "Other  180  12,839  45.22  13.13  181  26,777 ..."

        Columns: [#Loc] [#Units] WinAmount [%Chg] WinPercent [then 3-month, 12-month]
        """
        # Remove the label prefix to get just numbers
        # First strip footnote marker
        cleaned = re.sub(r'^\(\d\)\s*', '', line.strip())
        # Strip "Sports - <label>" or just "<label>"
        cleaned = re.sub(r'^[Ss]ports\s*[-–]\s*', '', cleaned).strip()
        # Strip the sport name itself
        for key in SPORT_LABELS:
            pat = re.compile(r'^' + re.escape(key) + r'\s*', re.IGNORECASE)
            if pat.match(cleaned):
                cleaned = pat.sub('', cleaned).strip()
                break

        nums = self._extract_numbers(cleaned)
        if len(nums) < 2:
            return None

        # Determine where WinAmount is by analyzing leading numbers.
        # Standard lines have #Loc #Units (two equal small integers) before WinAmount.
        # "Other" lines may have just #Loc (one small integer), or none.
        win_idx = self._find_win_amount_index(nums)
        if win_idx is None or win_idx >= len(nums):
            return None

        ggr = self._thousands_to_dollars(nums[win_idx])

        # Find WinPercent after WinAmount
        win_pct = self._find_win_pct(nums, start=win_idx + 1)
        handle = self._derive_handle(ggr, win_pct)

        return {"ggr": ggr, "handle": handle}

    def _extract_mobile_line(self, lines: list[str]) -> dict | None:
        """
        Parse the "*** Sports - Mobile" line.

        Format: "*** Sports - Mobile  68  39,252  (8.14)  6.98  77  128,803 ..."
        Columns: #Units  WinAmount  [%Chg]  WinPercent  [then 3-month, 12-month]

        Note: Mobile has only #Units (no #Loc).
        Early months (when mobile was new) may lack %Chg.
        """
        for line in lines:
            upper = line.upper()
            if "SPORTS - MOBILE" not in upper and "SPORTS-MOBILE" not in upper:
                continue

            # Strip the prefix
            cleaned = re.sub(
                r'^\*+\s*[Ss]ports\s*[-–]\s*[Mm]obile\s*',
                '', line.strip(),
            ).strip()

            nums = self._extract_numbers(cleaned)
            if len(nums) < 2:
                return None

            # nums[0] = #Units, nums[1] = WinAmount
            ggr = self._thousands_to_dollars(nums[1])

            # Find WinPercent
            win_pct = self._find_win_pct(nums, start=2)
            handle = self._derive_handle(ggr, win_pct)

            return {"ggr": ggr, "handle": handle, "source_raw_line": line.strip()}

        return None

    # ------------------------------------------------------------------
    # Number extraction and parsing utilities
    # ------------------------------------------------------------------
    def _extract_numbers(self, text: str) -> list[str]:
        """
        Extract all number tokens from text.
        Handles: 12,345  -1,234  (1,234)  3.79  (47.62)  -326  0
        Returns raw string tokens for further parsing.
        """
        return re.findall(r'\([\d,]+(?:\.\d+)?\)|[-]?[\d,]+(?:\.\d+)?', text)

    def _parse_number(self, token: str) -> float | None:
        """Parse a single number token. Parenthesized = negative."""
        if token is None:
            return None
        s = token.strip().replace(',', '')
        neg = False
        if s.startswith('(') and s.endswith(')'):
            neg = True
            s = s[1:-1]
        elif s.startswith('-'):
            neg = True
            s = s[1:]
        s = s.strip()
        if not s:
            return None
        try:
            val = float(s)
            return -val if neg else val
        except ValueError:
            return None

    def _thousands_to_dollars(self, token: str) -> float | None:
        """Parse a token that is in THOUSANDS and convert to dollars."""
        val = self._parse_number(token)
        if val is None:
            return None
        return val * 1000.0

    def _find_win_amount_index(self, nums: list[str]) -> int | None:
        """
        Determine the index of WinAmount in a list of number tokens.

        Standard sport lines: #Loc #Units WinAmount ... -> index 2
        "Other" lines:        #Loc WinAmount ...        -> index 1
        No-location lines:    WinAmount ...             -> index 0

        Heuristic: #Loc and #Units are small equal integers (< 500, no decimal).
        """
        if len(nums) < 2:
            return 0 if nums else None

        v0 = self._parse_number(nums[0])
        v1 = self._parse_number(nums[1])

        # Check for #Loc #Units pair: two similar small positive integers (within ±10)
        if (v0 is not None and v1 is not None
                and abs(v0) < 500 and abs(v1) < 500
                and '.' not in nums[0] and '.' not in nums[1]
                and abs(v0 - v1) <= 10 and len(nums) >= 3):
            return 2

        # Check for single #Loc (small integer, no decimal)
        if (v0 is not None and abs(v0) < 500
                and '.' not in nums[0]
                and v0 > 0 and len(nums) >= 2):
            # Could be #Loc without #Units (the "Other" pattern),
            # or it could be that WinAmount itself is small.
            # If v1 looks like a large WinAmount (has comma, or > 500),
            # treat v0 as #Loc.
            if ',' in nums[1] or (self._parse_number(nums[1]) is not None
                                   and abs(self._parse_number(nums[1])) >= 500):
                return 1
            # If both are small, could be ambiguous — default to index 0
            return 0

        return 0

    def _find_win_pct(self, nums: list[str], start: int) -> float | None:
        """
        Find WinPercent in the number list starting from `start`.

        After WinAmount, the remaining Current Month numbers are either:
          [%Chg, WinPercent, ...next_period...] (5-number group)
          [WinPercent, ...next_period...] (4-number group)

        Heuristic: WinPercent has a decimal point and is typically between
        -100 and 100. %Chg also has a decimal, but is followed by WinPercent.
        The key distinction is that after %Chg comes another decimal (WinPercent),
        but after WinPercent comes an integer (#Loc of next period).

        Simple approach: if nums[start] and nums[start+1] both have decimals,
        assume 5-number group and take nums[start+1] as WinPercent.
        Otherwise take nums[start] as WinPercent.
        """
        remaining = nums[start:]
        if not remaining:
            return None

        if len(remaining) >= 2 and '.' in remaining[0] and '.' in remaining[1]:
            # Both have decimals: first is %Chg, second is WinPercent
            return self._parse_number(remaining[1])

        if '.' in remaining[0]:
            # Only one decimal number immediately: it's WinPercent
            # But check if it's actually %Chg by seeing if a decimal follows
            return self._parse_number(remaining[0])

        # No decimal — might be a %Chg that's an integer (rare) followed by WinPercent
        if len(remaining) >= 2 and '.' in remaining[1]:
            return self._parse_number(remaining[1])

        return None

    def _derive_handle(self, ggr: float | None, win_pct: float | None) -> float | None:
        """
        Derive handle from GGR and Win Percent.
        Win Percent = GGR / Handle * 100
        Handle = GGR / (Win Percent / 100)
        """
        if ggr is None or win_pct is None or win_pct == 0.0:
            return None
        handle = ggr / (win_pct / 100.0)
        return abs(handle)

    # ------------------------------------------------------------------
    # Row building
    # ------------------------------------------------------------------
    def _build_row(
        self,
        period_end: date,
        channel: str,
        sport_category: str | None,
        ggr: float | None,
        handle: float | None,
    ) -> dict:
        """Build a single output row dict."""
        payouts = None
        if handle is not None and ggr is not None:
            payouts = handle - ggr

        tax_paid = None
        if ggr is not None and ggr > 0:
            tax_paid = ggr * NV_TAX_RATE

        return {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "ALL",
            "channel": channel,
            "sport_category": sport_category,
            "handle": handle,
            "gross_revenue": ggr,
            "standard_ggr": ggr,
            "payouts": payouts,
            "tax_paid": tax_paid,
        }


if __name__ == "__main__":
    scraper = NVScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"NV SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Sports: {df['sport_category'].value_counts().to_dict()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")

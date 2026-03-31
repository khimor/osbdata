"""
Vermont Sports Wagering Scraper
Source: liquorandlottery.vermont.gov monthly PDF reports
Format: PDF (DLL Sports Wagering Summary Report)
Launch: January 2024 (went live Jan 11, 2024)
Tax: ~31.7% revenue share on adjusted gross sports wagering revenue
Note: Online only; single operator platform with 6 skin operators
      (currently 3 active: FanDuel, DraftKings, Fanatics).

GGR computation:
  - standard_ggr  = Handle − Payouts
  - gross_revenue = Adjusted Gross Sports Wagering Revenue
                  = Handle − Payouts − (Resettlements + Voids + Fed Excise Tax + Promo)
  - promo_credits = "Less: Resettlements, Voids, Tax, Promo" (combined deduction line)
  - tax_paid      = Vermont Revenue Share
"""

import sys
import re
import io
import os
import subprocess
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
from scrapers.scraper_utils import setup_logger

VT_BASE_URL = "https://liquorandlottery.vermont.gov"

# Year-specific report listing pages
VT_YEAR_PAGE_TEMPLATE = VT_BASE_URL + "/monthly-sports-wagering-reports-{year}"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

MONTH_ABBREVS = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR",
    5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG",
    9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}

# VT launched Jan 2024 (sports wagering went live Jan 11, 2024)
VT_START_YEAR = 2024
VT_START_MONTH = 1


class VTScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("VT")
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_periods(self) -> list[dict]:
        """
        Discover VT sports wagering report periods.
        Scrapes the year-specific listing pages for PDF links, then generates
        a period for every month from launch to today.
        """
        discovered_urls = self._discover_from_listing_pages()

        periods = []
        today = date.today()
        year, month = VT_START_YEAR, VT_START_MONTH

        while True:
            last_day = calendar.monthrange(year, month)[1]
            period_end = date(year, month, last_day)
            if period_end > today:
                break

            key = f"{month}_{year}"
            download_url = discovered_urls.get(key)

            periods.append({
                "period_end": period_end,
                "period_type": "monthly",
                "year": year,
                "month": month,
                "download_url": download_url,
            })

            month += 1
            if month > 12:
                month = 1
                year += 1

        self.logger.info(
            f"  Generated {len(periods)} monthly periods "
            f"({len(discovered_urls)} URLs discovered from listing pages)"
        )
        return periods

    def _discover_from_listing_pages(self) -> dict:
        """
        Scrape VT year-specific report listing pages for PDF links.
        Pages: /monthly-sports-wagering-reports-{year}
        Returns dict of {month_year_key: url}.
        """
        urls = {}
        today = date.today()

        for year in range(VT_START_YEAR, today.year + 1):
            page_url = VT_YEAR_PAGE_TEMPLATE.format(year=year)
            try:
                resp = self._session.get(page_url, timeout=30)
                if resp.status_code != 200:
                    self.logger.warning(f"  Status {resp.status_code} from {page_url}")
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")

                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    text = link.get_text(strip=True).lower()
                    combined = text + " " + href.lower()

                    # Direct PDF links
                    if ".pdf" in href.lower():
                        period = self._extract_period_from_link(combined, year)
                        if period:
                            m, y = period
                            full_url = urljoin(page_url, href)
                            urls[f"{m}_{y}"] = full_url

                    # Drupal /document/ node links (Oct/Nov 2025 use this pattern)
                    elif "/document/" in href.lower():
                        period = self._extract_period_from_link(combined, year)
                        if period:
                            m, y = period
                            pdf_url = self._resolve_document_link(urljoin(page_url, href))
                            if pdf_url:
                                urls[f"{m}_{y}"] = pdf_url

            except Exception as e:
                self.logger.warning(f"  Failed to scrape {page_url}: {e}")

        return urls

    def _resolve_document_link(self, doc_url: str) -> str | None:
        """Follow a /document/ Drupal node page to find the actual PDF URL."""
        try:
            resp = self._session.get(doc_url, timeout=30)
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                if ".pdf" in link["href"].lower():
                    return urljoin(doc_url, link["href"])
        except Exception as e:
            self.logger.warning(f"  Failed to resolve document link {doc_url}: {e}")
        return None

    def _extract_period_from_link(self, text: str, fallback_year: int = None) -> tuple[int, int] | None:
        """Extract (month_num, year) from link text or URL."""
        text_lower = text.lower()

        # Try full month names first
        for name, num in MONTH_NAMES.items():
            if len(name) < 4:
                continue  # skip abbreviations in this pass
            if name in text_lower:
                year_match = re.search(r'20(2[4-9]|3[0-9])', text_lower)
                if year_match:
                    return (num, int("20" + year_match.group(1)))
                elif fallback_year:
                    return (num, fallback_year)

        # Try abbreviated months in URL patterns like "SWExecSumRptAPR2024"
        abbrev_match = re.search(
            r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*(\d{4})',
            text_lower
        )
        if abbrev_match:
            m_name = abbrev_match.group(1)
            yr = int(abbrev_match.group(2))
            if m_name in MONTH_NAMES:
                return (MONTH_NAMES[m_name], yr)

        return None

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_report(self, period_info: dict) -> Path:
        """Download VT sports wagering PDF."""
        year = period_info["year"]
        month = period_info["month"]
        filename = f"VT_{year}_{month:02d}.pdf"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            return save_path

        # Try discovered URL first
        download_url = period_info.get("download_url")
        if download_url:
            try:
                resp = self._session.get(download_url, timeout=60)
                if resp.status_code == 200 and len(resp.content) > 500:
                    with open(save_path, "wb") as f:
                        f.write(resp.content)
                    self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
                    return save_path
            except Exception as e:
                self.logger.warning(f"  Discovered URL failed: {e}")

        # Try candidate URLs based on known patterns
        for url in self._generate_candidate_urls(year, month):
            try:
                resp = self._session.get(url, timeout=30)
                if resp.status_code == 200 and len(resp.content) > 500:
                    with open(save_path, "wb") as f:
                        f.write(resp.content)
                    self.logger.info(f"  Downloaded: {filename} from {url}")
                    return save_path
            except Exception:
                continue

        raise FileNotFoundError(
            f"VT PDF not found for {MONTH_ABBREVS[month]} {year}"
        )

    def _generate_candidate_urls(self, year: int, month: int) -> list[str]:
        """Generate candidate PDF URLs using known VT naming patterns."""
        base = "https://liquorandlottery.vermont.gov/sites/liqlot/files/documents/"
        abbrev = MONTH_ABBREVS[month]
        full_name = calendar.month_name[month]
        candidates = []

        # 2026+ pattern: SW_Exec_Sum_Rpt_Month_Year.pdf
        candidates.append(f"{base}SW_Exec_Sum_Rpt_{full_name}_{year}.pdf")

        # 2024-2025 pattern: SWExecSumRpt{Month}{Year}.pdf
        candidates.append(f"{base}SWExecSumRpt{full_name}{year}.pdf")
        candidates.append(f"{base}SWExecSumRpt{abbrev.lower()}{year}.pdf")
        candidates.append(f"{base}SWExecSumRpt{abbrev}{year}.pdf")

        # With various timestamp suffixes
        for suffix in ["1", "11", "111", "1111", "51024", "11024"]:
            candidates.append(f"{base}SWExecSumRpt{full_name}{year}{suffix}.pdf")
            candidates.append(f"{base}SWExecSumRpt{abbrev.lower()}{year}{suffix}.pdf")

        # Updated prefix variant
        candidates.append(f"{base}UpdatedSWExecSumRpt{full_name}{year}111.pdf")

        return candidates

    # ------------------------------------------------------------------
    # Text extraction (with OCR fallback for CID-encoded fonts)
    # ------------------------------------------------------------------

    def _extract_text(self, file_path: Path) -> str:
        """
        Extract text from a VT PDF. Falls back to OCR if pdfplumber
        produces garbled CID-encoded text.
        """
        try:
            pdf = pdfplumber.open(file_path)
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text() or ""
                full_text += text + "\n"
            pdf.close()
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return ""

        # Detect garbled CID-encoded fonts: labels like "handle", "revenue",
        # "payouts" should appear in readable text but won't in garbled PDFs
        has_readable_labels = any(
            kw in full_text.lower()
            for kw in ["handle", "revenue", "payouts", "payout", "wagering"]
        )

        if has_readable_labels:
            return full_text

        # Fallback: OCR via tesseract
        self.logger.info(f"  CID-encoded font detected, falling back to OCR for {file_path.name}")
        return self._ocr_pdf(file_path)

    def _ocr_pdf(self, file_path: Path) -> str:
        """Render PDF to image and OCR with tesseract."""
        try:
            import pytesseract
            from PIL import Image
        except ImportError:
            self.logger.error("  pytesseract/Pillow not installed, cannot OCR")
            return ""

        try:
            prefix = f"/tmp/vt_ocr_{file_path.stem}"
            subprocess.run(
                ["pdftoppm", "-png", "-r", "300", str(file_path), prefix],
                capture_output=True, timeout=30,
            )

            full_text = ""
            for f in sorted(os.listdir("/tmp")):
                if f.startswith(os.path.basename(prefix)) and f.endswith(".png"):
                    img_path = f"/tmp/{f}"
                    img = Image.open(img_path)
                    text = pytesseract.image_to_string(img)
                    full_text += text + "\n"
                    os.unlink(img_path)

            return full_text
        except Exception as e:
            self.logger.error(f"  OCR failed for {file_path}: {e}")
            return ""

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """
        Parse VT DLL Sports Wagering Summary Report PDF.

        All reports (Jan 2024 – present) use the same single-page layout:
          Section 1: VERMONT REVENUE SHARE
            - Total Handle Receipts
            - Less: Winning Payouts
            - Less: Resettlements, Voids, Tax, Promo
            - Adjusted Gross Sports Wagering Revenue
            - Revenue Share Rate
            - Vermont Revenue Share
          Section 2: HANDLE BY SPORT (top 5 + All Other)
          Section 3: HANDLES / BETS (in-state vs out-of-state)

        Mapping to standard columns:
          handle         = Total Handle Receipts
          payouts        = Less: Winning Payouts
          promo_credits  = Less: Resettlements, Voids, Tax, Promo (combined deductions)
          gross_revenue  = Adjusted Gross Sports Wagering Revenue (adjusted GGR)
          standard_ggr   = handle − payouts (auto-computed by base scraper)
          tax_paid       = Vermont Revenue Share
        """
        period_end = period_info["period_end"]

        full_text = self._extract_text(file_path)

        if not full_text.strip():
            self.logger.warning(f"  No text extracted from {file_path}")
            return pd.DataFrame()

        # Parse the revenue share section
        revenue_data = self._parse_revenue_section(full_text)

        if not revenue_data.get("handle") and not revenue_data.get("gross_revenue"):
            self.logger.warning(f"  Could not extract revenue data from {file_path}")
            return pd.DataFrame()

        # Build source_raw_line from the parsed revenue values
        revenue_parts = []
        for key in ["handle", "payouts", "deductions", "adjusted_ggr", "revenue_share"]:
            val = revenue_data.get(key)
            if val is not None:
                revenue_parts.append(f"{key}={val}")

        # -- Aggregate row (statewide totals) --
        agg_row = {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "ALL",
            "channel": "online",
            "sport_category": None,
            "handle": revenue_data.get("handle"),
            "payouts": revenue_data.get("payouts"),
            "promo_credits": revenue_data.get("deductions"),
            "gross_revenue": revenue_data.get("adjusted_ggr"),
            "tax_paid": revenue_data.get("revenue_share"),
            "source_raw_line": ' | '.join(revenue_parts) if revenue_parts else None,
        }

        # Explicitly set standard_ggr = handle - payouts
        if agg_row["handle"] is not None and agg_row["payouts"] is not None:
            agg_row["standard_ggr"] = agg_row["handle"] - agg_row["payouts"]

        rows = [agg_row]

        # -- Sport-level rows (handle only) --
        sport_rows = self._parse_sport_section(full_text, period_end)
        rows.extend(sport_rows)

        result = pd.DataFrame(rows)

        # Source provenance fields
        result["source_file"] = file_path.name
        result["source_url"] = period_info.get('download_url', period_info.get('url', None))
        result["source_page"] = 1  # Single-page PDF

        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))

        self.logger.info(
            f"  Parsed: handle=${revenue_data.get('handle', 0):,.0f}  "
            f"payouts=${revenue_data.get('payouts', 0):,.0f}  "
            f"deductions=${revenue_data.get('deductions', 0):,.0f}  "
            f"std_ggr=${agg_row.get('standard_ggr', 0):,.0f}  "
            f"adj_ggr=${revenue_data.get('adjusted_ggr', 0):,.0f}  "
            f"tax=${revenue_data.get('revenue_share', 0):,.0f}  "
            f"sports={len(sport_rows)}"
        )

        return result

    def _parse_sport_section(self, text: str, period_end: date) -> list[dict]:
        """
        Parse the HANDLE BY SPORT section.
        Each report lists top 5 sports + "All Other" with handle amounts.

        Format (pdfplumber):
          #1 Basketball $7,639,100
          #2 Soccer $3,214,248
          ...
          All Other All Other $5,640,534

        Format (OCR, may split labels/values across lines):
          Basketball
          Soccer
          ...
          $7,639,100
          $3,214,248
          ...
        """
        rows = []

        # Extract the sport section text
        sport_match = re.search(
            r'HANDLE\s+BY\s+SPORT(.*?)(?:HANDLES?\s*/?\s*BETS|$)',
            text, re.DOTALL | re.IGNORECASE
        )
        if not sport_match:
            return rows

        section = sport_match.group(1)
        lines = section.splitlines()

        # Strategy 1: lines with both sport name and dollar amount
        # e.g., "#1 Basketball $7,639,100" or "All Other All Other $5,640,534"
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            dollars = re.findall(r'\$[\d,]+(?:\.\d+)?', stripped)
            if not dollars:
                continue

            # Skip total lines: "Handle $21,534,379" or "Handle | $18,423,720"
            if re.match(r'^\s*(?:Handle|Total)\s', stripped, re.IGNORECASE):
                continue

            # Extract sport name: everything before the dollar sign, cleaned up
            name_part = re.sub(r'\$[\d,]+.*', '', stripped).strip()
            # Remove rank prefix like "#1 " or "#2 "
            name_part = re.sub(r'^#\d+\s*', '', name_part).strip()
            # Remove duplicate "All Other All Other" -> "All Other"
            name_part = re.sub(r'^(All\s+Other)\s+\1$', r'\1', name_part, flags=re.IGNORECASE)

            if not name_part:
                continue

            handle = self._parse_money(dollars[0])
            if handle is not None and handle > 0:
                rows.append({
                    "period_end": period_end,
                    "period_type": "monthly",
                    "operator_raw": "ALL",
                    "channel": "online",
                    "sport_category": name_part,
                    "handle": handle,
                    "source_raw_line": stripped,
                })

        # Strategy 2 (OCR fallback): sport names and dollar values on separate lines
        if not rows:
            sport_names = []
            sport_handles = []
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                dollars = re.findall(r'\$[\d,]+(?:\.\d+)?', stripped)
                if dollars:
                    # Skip total/header
                    if re.match(r'^\s*(?:Handle|Total)\s', stripped, re.IGNORECASE):
                        continue
                    sport_handles.append(self._parse_money(dollars[0]))
                elif re.match(r'^[A-Za-z]', stripped):
                    # Skip header lines
                    if any(kw in stripped.lower() for kw in [
                        'sport rank', 'sport name', 'handle by', '|'
                    ]):
                        continue
                    sport_names.append(stripped)

            # Match names to handles positionally
            for name, handle in zip(sport_names, sport_handles):
                if handle is not None and handle > 0:
                    rows.append({
                        "period_end": period_end,
                        "period_type": "monthly",
                        "operator_raw": "ALL",
                        "channel": "online",
                        "sport_category": name,
                        "handle": handle,
                        "source_raw_line": name,
                    })

        # Pre-aggregate rows whose sport_category will normalize to the same
        # value (e.g. "eBasketball" and "All Other" both → "other"), so the
        # base scraper's dedup doesn't silently discard handle data.
        if rows:
            rows = self._aggregate_sport_collisions(rows)

        return rows

    def _aggregate_sport_collisions(self, rows: list[dict]) -> list[dict]:
        """
        If two raw sport names (e.g. 'eBasketball', 'All Other') will both
        normalize to the same value (e.g. 'other'), sum their handles into
        one row so no data is lost during dedup.
        """
        from scrapers.operator_mapping import normalize_sport

        # Group by normalized sport name
        groups: dict[str, list[dict]] = {}
        for row in rows:
            norm = normalize_sport(row["sport_category"])
            groups.setdefault(norm, []).append(row)

        merged = []
        for norm, group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                # Sum handles, keep first row as template
                combined = group[0].copy()
                combined["handle"] = sum(r["handle"] for r in group if r["handle"])
                # Use the normalized name to avoid confusion
                combined["sport_category"] = norm
                merged.append(combined)

        return merged

    def _parse_revenue_section(self, text: str) -> dict:
        """
        Parse the VERMONT REVENUE SHARE section from the PDF text.

        Uses two strategies:
        1. Label-matching: find keywords and their associated dollar values
        2. Positional fallback: extract all dollar values from the revenue share
           section (between "REVENUE SHARE" and "HANDLE BY SPORT") and map them
           by their known order: handle, payouts, deductions, adjusted_ggr, revenue_share

        Returns dict with keys: handle, payouts, deductions, adjusted_ggr,
        revenue_share_rate, revenue_share.
        """
        # Strategy 1: label-based matching
        result = self._parse_revenue_by_labels(text)

        # Sanity check: payouts must be less than handle
        label_suspect = False
        if (result.get("handle") is not None and result.get("payouts") is not None
                and result.get("payouts") >= result.get("handle")):
            self.logger.info("  Label match: payouts >= handle, discarding label payouts")
            label_suspect = True

        # If we got all key fields and they pass sanity check, we're done
        if (not label_suspect
                and result.get("handle") is not None
                and result.get("payouts") is not None
                and result.get("adjusted_ggr") is not None):
            return result

        # Strategy 2: positional extraction from the revenue share section
        self.logger.info("  Using positional extraction for revenue section")
        positional = self._parse_revenue_positional(text)

        if label_suspect and positional:
            # Positional is more reliable — use it as the base, fill gaps from labels
            for key in ["handle", "payouts", "deductions", "adjusted_ggr",
                         "revenue_share_rate", "revenue_share"]:
                if positional.get(key) is None and result.get(key) is not None:
                    positional[key] = result[key]
            result = positional
        else:
            # Merge: use label-based values where available, fill gaps with positional
            for key in ["handle", "payouts", "deductions", "adjusted_ggr",
                         "revenue_share_rate", "revenue_share"]:
                if result.get(key) is None and positional.get(key) is not None:
                    result[key] = positional[key]

        # Cross-check: compute adjusted_ggr if missing
        if (result.get("adjusted_ggr") is None
                and result.get("handle") is not None
                and result.get("payouts") is not None
                and result.get("deductions") is not None):
            result["adjusted_ggr"] = (
                result["handle"] - result["payouts"] - result["deductions"]
            )

        return result

    def _parse_revenue_by_labels(self, text: str) -> dict:
        """Parse revenue section using keyword matching on lines."""
        result = {}
        lines = text.splitlines()

        def _find_dollars(line_text):
            d = re.findall(r'[\$\(][\d,]+(?:\.\d+)?[\)]?', line_text)
            if not d:
                d = re.findall(r'(?<!\d)([\d,]+\.\d{2})(?!\d)', line_text)
            return d

        def _lookahead_dollars(idx):
            for j in range(idx + 1, min(idx + 5, len(lines))):
                next_line = lines[j].strip()
                if not next_line:
                    continue
                d = _find_dollars(next_line)
                if d:
                    return d
            return []

        for i, line in enumerate(lines):
            lower = line.strip().lower()
            dollars = _find_dollars(line)

            if ("handle" in lower and "receipt" in lower) or (
                    "total handle" in lower and "sport" not in lower):
                if not dollars:
                    dollars = _lookahead_dollars(i)
                if dollars:
                    result["handle"] = self._parse_money(dollars[0])

            elif "winning" in lower and "payout" in lower:
                if not dollars:
                    dollars = _lookahead_dollars(i)
                if dollars:
                    result["payouts"] = self._parse_money(dollars[0])

            elif "resettlement" in lower or ("voids" in lower and "promo" in lower):
                if not dollars:
                    dollars = _lookahead_dollars(i)
                if dollars:
                    result["deductions"] = self._parse_money(dollars[0])

            elif "adjusted" in lower and "gross" in lower and "revenue" in lower:
                if not dollars:
                    dollars = _lookahead_dollars(i)
                if dollars:
                    result["adjusted_ggr"] = self._parse_money(dollars[0])

            elif "revenue share rate" in lower:
                pct_match = re.search(r'(\d+\.?\d*)%', line)
                if not pct_match:
                    for j in range(i + 1, min(i + 3, len(lines))):
                        pct_match = re.search(r'(\d+\.?\d*)%', lines[j])
                        if pct_match:
                            break
                if pct_match:
                    result["revenue_share_rate"] = float(pct_match.group(1))

            elif ("vermont" in lower and "revenue share" in lower
                  and "rate" not in lower and "adjusted" not in lower):
                if not dollars:
                    dollars = _lookahead_dollars(i)
                if dollars:
                    result["revenue_share"] = self._parse_money(dollars[0])

        return result

    def _parse_revenue_positional(self, text: str) -> dict:
        """
        Fallback: extract revenue data by position.
        In the VERMONT REVENUE SHARE section, dollar values always appear in order:
          1. Handle, 2. Payouts, 3. Deductions, 4. Adjusted GGR, 5. Revenue Share
        The percentage (Revenue Share Rate) appears between #4 and #5.
        """
        result = {}

        # Extract text between REVENUE SHARE and HANDLE BY SPORT
        revenue_match = re.search(
            r'(?:REVENUE\s+SHARE|revenue\s+share).*?(?:HANDLE\s+BY\s+SPORT|handle\s+by\s+sport)',
            text, re.DOTALL | re.IGNORECASE
        )
        if not revenue_match:
            return result

        section = revenue_match.group(0)

        # Find all dollar values in order
        dollar_values = re.findall(r'[\$][\d,]+(?:\.\d+)?', section)
        # Find percentage
        pct_match = re.search(r'(\d+\.?\d*)%', section)

        # Map positionally: handle, payouts, deductions, adjusted_ggr, revenue_share
        field_names = ["handle", "payouts", "deductions", "adjusted_ggr", "revenue_share"]
        for idx, field in enumerate(field_names):
            if idx < len(dollar_values):
                result[field] = self._parse_money(dollar_values[idx])

        if pct_match:
            result["revenue_share_rate"] = float(pct_match.group(1))

        return result

    def _parse_money(self, value) -> float | None:
        """Parse money from VT PDF text."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        is_negative = False
        if s.startswith('(') and s.endswith(')'):
            is_negative = True
            s = s[1:-1]
        s = s.replace('$', '').replace(',', '').strip()
        if not s or s in ('-', 'N/A', '', '--', '-0-'):
            return None
        try:
            val = float(s)
            return -val if is_negative else val
        except ValueError:
            return None


if __name__ == "__main__":
    scraper = VTScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"VT SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")

        money_cols = ['handle', 'payouts', 'standard_ggr', 'promo_credits',
                      'gross_revenue', 'tax_paid']
        for col in money_cols:
            if col in df.columns and df[col].notna().any():
                vals = df[col].dropna()
                # Values are in cents from base scraper normalization
                print(f"  {col}: ${vals.sum()/100:,.0f} total across {len(vals)} periods")

        print(f"\nSample rows (last 3):")
        display_cols = ['period_end', 'handle', 'payouts', 'standard_ggr',
                        'promo_credits', 'gross_revenue', 'tax_paid']
        display_cols = [c for c in display_cols if c in df.columns]
        print(df[display_cols].tail(3).to_string(index=False))

"""
Arizona Event Wagering Scraper
Source: gaming.az.gov monthly PDF reports
Format: PDF (operator-level data with retail/mobile split)
Launch: September 2021
Tax: 8% retail, 10% online on adjusted gross event wagering receipts
Note: Uses Playwright with stealth settings to bypass Cloudflare protection on
      gaming.az.gov. Discovers reports via paginated blog listing, then follows
      links to individual report pages to find PDF download URLs. PDF parsing
      splits on '$' signs to extract operator names and financial values.
"""

import sys
import re
import calendar
import time
from pathlib import Path
from datetime import date
from urllib.parse import urljoin

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger

# Reports listing URL (paginated: ?page=0 through ?page=6+)
AZ_REPORTS_INDEX = "https://gaming.az.gov/blog-terms/event-wagering-revenue-reports"
AZ_MAX_PAGES = 10  # Maximum pagination pages to check

AZ_BASE_URL = "https://gaming.az.gov"

# Arizona launch: September 2021
AZ_START_YEAR = 2021
AZ_START_MONTH = 9

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Reverse lookup: month number -> full name
MONTH_NUM_TO_NAME = {v: k for k, v in MONTH_NAMES.items()}


STEALTH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]
STEALTH_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


class AZScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("AZ")
        self._pw = None
        self._browser = None
        self._context = None

    def _ensure_browser(self):
        """Lazily start Playwright browser with stealth settings."""
        if self._browser is not None:
            return
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=STEALTH_ARGS,
        )
        self._context = self._browser.new_context(
            user_agent=STEALTH_UA,
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True,
            accept_downloads=True,
        )
        self.logger.info("Started Playwright browser with stealth settings")

    def _close_browser(self):
        """Close Playwright browser and stop the instance."""
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
            self._context = None
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None

    def _fetch_page_html(self, url: str, wait_ms: int = 2000) -> str | None:
        """Fetch a page's HTML using the stealth Playwright browser."""
        self._ensure_browser()
        page = self._context.new_page()
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(wait_ms)
            html = page.content()
            return html
        except Exception as e:
            self.logger.warning(f"  Playwright failed to load {url}: {e}")
            return None
        finally:
            page.close()

    def run(self, backfill: bool = False) -> pd.DataFrame:
        """Override run to ensure Playwright browser is cleaned up."""
        self._backfill = backfill
        try:
            return super().run(backfill=backfill)
        finally:
            self._close_browser()

    # ------------------------------------------------------------------
    # discover_periods
    # ------------------------------------------------------------------
    def _get_existing_periods(self) -> set:
        """Read the CSV to find which periods we already have data for."""
        csv_path = Path("data/processed") / f"{self.state_code}.csv"
        if not csv_path.exists():
            return set()
        try:
            df = pd.read_csv(csv_path, usecols=['period_end'], low_memory=False)
            return set(df['period_end'].unique())
        except Exception:
            return set()

    def discover_periods(self) -> list[dict]:
        """
        Discover AZ report periods. Only crawls listing page 0 (newest first).
        Stops paginating once all found periods already exist in our data.
        Only visits individual report pages for new periods we don't have yet.
        """
        existing = set() if getattr(self, '_backfill', False) else self._get_existing_periods()
        report_pages = self._discover_report_page_links(existing)
        self.logger.info(f"  Found {len(report_pages)} report page links")

        periods = []
        for url, month_num, year in report_pages:
            last_day = calendar.monthrange(year, month_num)[1]
            period_end = date(year, month_num, last_day)
            period_str = str(period_end)

            if period_str in existing:
                # Already have this period - no need to visit report page
                periods.append({
                    "period_end": period_end,
                    "period_type": "monthly",
                    "year": year,
                    "month": month_num,
                    "month_name": MONTH_NUM_TO_NAME[month_num].capitalize(),
                    "report_page_url": url,
                    "download_url": None,
                })
                continue

            # New period - visit report page to find PDF URL
            pdf_url = self._find_pdf_on_report_page(url)
            periods.append({
                "period_end": period_end,
                "period_type": "monthly",
                "year": year,
                "month": month_num,
                "month_name": MONTH_NUM_TO_NAME[month_num].capitalize(),
                "report_page_url": url,
                "download_url": pdf_url,
            })

        periods.sort(key=lambda p: p["period_end"])
        new_count = sum(1 for p in periods if p.get('download_url'))
        self.logger.info(f"  {len(periods)} total periods, {new_count} new")
        return periods

    def _discover_report_page_links(self, existing_periods: set) -> list[tuple]:
        """
        Crawl listing pages to find report links. Starts from page 0 (newest).
        Stops paginating once a full page of results are all already in our data.
        Uses Playwright to bypass Cloudflare.
        """
        results = []
        seen_urls = set()

        for page_num in range(AZ_MAX_PAGES):
            url = f"{AZ_REPORTS_INDEX}?page={page_num}"
            html = self._fetch_page_html(url)

            if html is None:
                self.logger.warning(f"  Failed to fetch listing page {page_num}")
                break

            soup = BeautifulSoup(html, "html.parser")
            links_found = 0
            all_existing = True

            for link in soup.find_all("a", href=True):
                href = link["href"]
                match = re.search(
                    r'/(?:resources/reports/)?event-wagering-revenue-report-'
                    r'(\w+)-(\d{4})',
                    href, re.IGNORECASE
                )
                if not match:
                    continue

                month_str = match.group(1).lower()
                year = int(match.group(2))

                if month_str not in MONTH_NAMES:
                    continue

                month_num = MONTH_NAMES[month_str]
                full_url = urljoin(AZ_BASE_URL, href)

                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    results.append((full_url, month_num, year))
                    links_found += 1

                    # Check if this period already exists
                    last_day = calendar.monthrange(year, month_num)[1]
                    period_str = str(date(year, month_num, last_day))
                    if period_str not in existing_periods:
                        all_existing = False

            self.logger.info(f"  Page {page_num}: {links_found} links")

            if links_found == 0 and page_num > 0:
                break

            # Stop paginating if everything on this page already exists
            if all_existing and links_found > 0 and page_num > 0:
                self.logger.info(f"  All periods on page {page_num} already exist, stopping")
                break

            time.sleep(0.5)

        return results

    def _find_pdf_on_report_page(self, report_page_url: str) -> str | None:
        """
        Fetch an individual report page and find the PDF download link.
        Returns the full PDF URL, or None if not found.
        Uses Playwright to bypass Cloudflare.
        """
        html = self._fetch_page_html(report_page_url)
        if html is None:
            return None

        soup = BeautifulSoup(html, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.lower().endswith(".pdf"):
                full_url = urljoin(AZ_BASE_URL, href)
                return full_url

        self.logger.warning(f"  No PDF link found on {report_page_url}")
        return None

    # ------------------------------------------------------------------
    # download_report
    # ------------------------------------------------------------------
    def download_report(self, period_info: dict) -> Path:
        """Download AZ event wagering PDF for a month using Playwright."""
        year = period_info["year"]
        month = period_info["month"]
        filename = f"AZ_{year}_{month:02d}.pdf"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            # Validate that cached file is actually a PDF
            with open(save_path, "rb") as f:
                header = f.read(5)
            if header == b"%PDF-":
                return save_path
            else:
                self.logger.warning(
                    f"  Cached file {filename} is not a valid PDF (header: {header!r}), "
                    f"re-downloading"
                )
                save_path.unlink()

        download_url = period_info.get("download_url")
        if not download_url:
            # PDF URL wasn't looked up during discovery (cached period) - fetch it now
            report_page_url = period_info.get("report_page_url")
            if report_page_url:
                download_url = self._find_pdf_on_report_page(report_page_url)
            if not download_url:
                raise FileNotFoundError(
                    f"No PDF URL discovered for {period_info['month_name']} {year}. "
                    f"Report page: {period_info.get('report_page_url', 'unknown')}"
                )

        try:
            self._ensure_browser()
            # Playwright triggers a download for PDF URLs; use expect_download
            # with evaluate() to avoid goto's "Download is starting" error
            page = self._context.new_page()
            try:
                with page.expect_download(timeout=60000) as download_info:
                    page.evaluate("url => window.location.href = url", download_url)
                download = download_info.value
                download.save_as(save_path)
                content = save_path.read_bytes()
            finally:
                page.close()

            if len(content) < 1000:
                raise ValueError(
                    f"PDF too small ({len(content)} bytes) - "
                    f"likely an error page, not a real PDF"
                )

            if not content[:5] == b"%PDF-":
                raise ValueError(
                    f"Downloaded content is not a PDF "
                    f"(starts with {content[:20]!r})"
                )

            with open(save_path, "wb") as f:
                f.write(content)

            self.logger.info(
                f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes) "
                f"from {download_url}"
            )
            return save_path

        except Exception as e:
            raise FileNotFoundError(
                f"Failed to download PDF for {period_info['month_name']} {year}: {e}"
            ) from e

    # ------------------------------------------------------------------
    # parse_report
    # ------------------------------------------------------------------
    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """
        Parse AZ event wagering PDF using text-based '$' splitting.

        The PDF has an "Operator:" section header line containing both
        "Operator:" and column headers like "Retail" / "Mobile".
        Subsequent lines are operator data where values are separated by '$' signs.

        12 values = retail + mobile (6 metrics per channel):
            values[0]=retail handle, values[1]=mobile handle,
            values[4]=retail adj_gross, values[5]=mobile adj_gross

        6 values = mobile only:
            values[0]=handle, values[2]=adj_gross
        """
        period_end = period_info["period_end"]

        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        # Capture page 1 as a PNG screenshot for provenance
        screenshot_path = self.capture_pdf_page(file_path, 1, period_info)

        # Extract all text from the PDF
        all_text = ""
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            all_text += page_text + "\n"
        pdf.close()

        if not all_text.strip():
            self.logger.warning(f"  No text extracted from {file_path}")
            return pd.DataFrame()

        rows = self._parse_text_dollar_split(all_text, period_end)

        if not rows:
            self.logger.warning(f"  No operator rows parsed from {file_path}")
            return pd.DataFrame()

        # Add source provenance to each row
        source_url = period_info.get('download_url', period_info.get('url', None))
        for row in rows:
            row["source_file"] = file_path.name
            row["source_page"] = None  # text merged across pages; per-row page not tracked
            row["source_table_index"] = None
            row["source_url"] = source_url

        result = pd.DataFrame(rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        if screenshot_path:
            result["source_screenshot"] = screenshot_path
        return result

    def _parse_text_dollar_split(self, text: str, period_end: date) -> list[dict]:
        """
        Parse PDF text using the '$' splitting technique from the old working scraper.

        Strategy:
        1. Find the "Operator:" header line (contains "Operator:" and "Retail" or "Mobile")
        2. Read subsequent lines until we hit a subtotal line (starts with "$") or
           "Limited" or "Total" section
        3. Split each operator line on "$" to get name + values
        4. Handle 12-value (retail+mobile) and 6-value (mobile-only) formats
        """
        lines = text.splitlines()
        rows = []

        in_operator_section = False
        skip_section = False  # True when in "Limited Event Wagering" section

        # Phrases that indicate non-operator summary/footer lines
        SKIP_LINE_PHRASES = [
            "net = adj gross",
            "privilege fees",
            "adjusted gross event wagering receipts subject",
            "free bets",
            "promotional credits",
            "annual audit",
            "calendar year",
            "fiscal year",
            "since inception",
            "all retail",
            "all event wagering",
            "gross event wagering receipts (wagers)",
            "winnings paid to players",
            "pursuant to",
            "these numbers",
            "department makes",
        ]

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            lower = stripped.lower()

            # Detect "Limited Event Wagering" section -- skip it
            if "limited event wagering" in lower:
                skip_section = True
                in_operator_section = False
                continue

            # Detect operator section header
            if "operator" in lower and ("retail" in lower or "mobile" in lower):
                # This is the header row for an operator data section
                if "limited" in lower:
                    skip_section = True
                    in_operator_section = False
                else:
                    in_operator_section = True
                    skip_section = False
                continue

            # If we are in a skip section, keep skipping until a new section header
            if skip_section:
                # Check if this might be a new non-limited section header
                if "operator" in lower and "limited" not in lower:
                    skip_section = False
                    in_operator_section = True
                continue

            if not in_operator_section:
                continue

            # Check for end of operator section
            # Subtotal lines start with "$"
            if stripped.startswith("$"):
                # This is a subtotal line -- end the operator section
                in_operator_section = False
                continue

            # "Total" lines end the section
            if lower.startswith("total") or lower.startswith("grand total"):
                in_operator_section = False
                continue

            # Skip non-data lines (headers, footnotes, etc.)
            if "$" not in stripped:
                continue

            # Skip summary/footer lines that contain "$" but are not operator data
            if any(phrase in lower for phrase in SKIP_LINE_PHRASES):
                continue

            # Parse the operator data line by splitting on "$"
            parsed = self._parse_operator_line(stripped, period_end)
            if parsed:
                for r in parsed:
                    r["source_raw_line"] = stripped
                rows.extend(parsed)

        return rows

    def _parse_operator_line(self, line: str, period_end: date) -> list[dict]:
        """
        Parse a single operator data line by splitting on '$'.

        Format: "OperatorName $val1 $val2 ... $valN"
        After splitting on '$':
          parts[0] = operator name
          parts[1:] = dollar values (as strings, may contain commas/parens)

        12 values = retail + mobile (6 metrics x 2 channels, interleaved retail/mobile):
          [0]  = retail handle (Gross EW Receipts)
          [1]  = mobile handle
          [2]  = retail payouts (Winnings Paid to Players)
          [3]  = mobile payouts
          [4]  = retail adj_gross (Adj Gross EW Receipts = Handle - Payouts - Excise)
          [5]  = mobile adj_gross
          [6]  = retail promo_credits (Free Bets Allowable Deduction)
          [7]  = mobile promo_credits
          [8]  = retail net_revenue (Adj Gross Subject to Privilege Fees)
          [9]  = mobile net_revenue
          [10] = retail tax_paid (Privilege Fees)
          [11] = mobile tax_paid

        6 values = mobile only (same 6 metrics, single channel):
          [0] = handle
          [1] = payouts
          [2] = adj_gross
          [3] = promo_credits
          [4] = net_revenue
          [5] = tax_paid
        """
        parts = line.split("$")
        if len(parts) < 2:
            return []

        operator_name = parts[0].strip()
        if not operator_name:
            return []

        # Clean up the operator name (remove trailing whitespace, numbers)
        operator_name = operator_name.rstrip()

        # Parse dollar values
        values = []
        for part in parts[1:]:
            val = self._clean_dollar_value(part)
            values.append(val)

        rows = []

        if len(values) >= 12:
            # Retail + Mobile format (6 metrics x 2 channels)
            for channel, offset in [("retail", 0), ("online", 1)]:
                handle = values[0 + offset]
                payouts = values[2 + offset]
                adj_gross = values[4 + offset]
                promo_credits = values[6 + offset]
                net_revenue = values[8 + offset]
                tax_paid = values[10 + offset]
                standard_ggr = (handle or 0) - (payouts or 0)
                federal_excise_tax = standard_ggr - (adj_gross or 0)

                rows.append({
                    "period_end": period_end,
                    "period_type": "monthly",
                    "operator_raw": operator_name,
                    "channel": channel,
                    "handle": handle,
                    "payouts": payouts,
                    "gross_revenue": adj_gross,
                    "standard_ggr": standard_ggr,
                    "promo_credits": promo_credits,
                    "net_revenue": net_revenue,
                    "tax_paid": tax_paid,
                    "federal_excise_tax": federal_excise_tax,
                })

        elif len(values) >= 6:
            # Mobile-only format (6 metrics, 1 channel)
            handle = values[0]
            payouts = values[1]
            adj_gross = values[2]
            promo_credits = values[3]
            net_revenue = values[4]
            tax_paid = values[5]
            standard_ggr = (handle or 0) - (payouts or 0)
            federal_excise_tax = standard_ggr - (adj_gross or 0)

            rows.append({
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": operator_name,
                "channel": "online",
                "handle": handle,
                "payouts": payouts,
                "gross_revenue": adj_gross,
                "standard_ggr": standard_ggr,
                "promo_credits": promo_credits,
                "net_revenue": net_revenue,
                "tax_paid": tax_paid,
                "federal_excise_tax": federal_excise_tax,
            })

        elif len(values) >= 2:
            # Minimal format -- at least handle and some revenue
            handle = values[0]
            adj_gross = values[-1] if len(values) >= 3 else values[1]

            rows.append({
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": operator_name,
                "channel": "online",
                "handle": handle,
                "gross_revenue": adj_gross,
            })

        return rows

    def _clean_dollar_value(self, raw: str) -> float | None:
        """
        Clean a dollar value string extracted after splitting on '$'.
        Examples: "1,234,567.89 ", "(123,456.78)", "- ", "-0-", "0.00"
        """
        s = raw.strip()

        # Handle empty or dash values
        if not s or s in ("-", "--", "---", "-0-", "N/A", "n/a"):
            return 0.0

        # Check for parenthetical negatives: (1,234.56)
        is_negative = False
        if s.startswith("(") and ")" in s:
            is_negative = True
            s = s[: s.index(")")].lstrip("(")

        # Remove commas, trailing whitespace, and any non-numeric trailing chars
        s = re.sub(r"[,\s]", "", s)
        # Take only the numeric portion (digits, dots, leading minus)
        match = re.match(r"^-?[\d.]+", s)
        if not match:
            return 0.0

        try:
            val = float(match.group())
            return -val if is_negative else val
        except ValueError:
            return 0.0


if __name__ == "__main__":
    scraper = AZScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"AZ SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        if 'operator_standard' in df.columns:
            print(f"Operators: {df['operator_standard'].nunique()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        if 'operator_standard' in df.columns:
            print(f"\nPer-operator row counts:")
            for op in sorted(df['operator_standard'].unique()):
                count = len(df[df['operator_standard'] == op])
                print(f"  {op}: {count}")
    else:
        print("No data scraped.")

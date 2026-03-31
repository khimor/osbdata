"""
Oregon Sports Betting Revenue Scraper
Source: Oregon Digital Collections Library — monthly PDF reports
Format: PDF (monthly sports betting/gaming activity reports)
Launch: October 2019 (Scoreboard app via DraftKings)
Tax: Lottery retains net proceeds (~49% to DraftKings per contract, Lottery keeps rest)
Note: DraftKings sole online operator via Lottery Scoreboard. Online only.
      Monthly PDFs available from Jan 2020 onward.

PDF format varies across eras:
  - 2020-2021: "Scoreboard Gaming Activity" with extra columns (# of Bets, etc.)
  - 2022: Split between DraftKings and Scoreboard platforms
  - 2023+: Single combined table, simpler format

All eras have consistent text lines: "Sport  Turnover  GGR  Margin(%)"
We parse from extracted text rather than table objects for reliability.
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
from scrapers.scraper_utils import setup_logger, clean_currency

# Oregon Digital Collections search URL template (paginated, 24 per page)
OR_SEARCH_URL = (
    "https://digitalcollections.library.oregon.gov/nodes/search/"
    "?orderby=node_id&order=desc&ntids=W10=&filter=eyJudGlkcyI6W10sImZhY2V0Ijp7fX0="
    "&a2z=W10=&page={page}&viewtype=grid&type=all&digital=0&in=0&access=0"
    "&has=W10=&bid=0"
    "&meta=eyIxMCI6WyJTcG9ydHMgYmV0dGluZyAtLSBPcmVnb24gLS0gU3RhdGlzdGljcyAtLSBQZXJpb2RpY2FscyJdfQ=="
    "&metainc=W10="
)

OR_PDF_URL = "https://digitalcollections.library.oregon.gov/assets/displaypdf/{node_id}"

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Regex to extract YYYY-MM from alt text like "2026-01 Oregon Lottery ..."
DATE_RE = re.compile(r'(\d{4})-(\d{2})')

# Regex to extract node_id from href like "nodes/view/316730"
NODE_RE = re.compile(r'nodes/view/(\d+)')


class ORScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("OR")

    # ------------------------------------------------------------------
    # discover_periods
    # ------------------------------------------------------------------
    def discover_periods(self) -> list[dict]:
        """
        Discover monthly PDF reports from Oregon Digital Collections.

        Fetches paginated search results, extracts (YYYY-MM, node_id) pairs
        from alt text and hrefs. Deduplicates by (year, month), keeping the
        first node_id per month. Skips YTD/annual items.
        """
        periods = []
        seen_months = {}  # (year, month) -> node_id

        for page in range(1, 10):  # Up to 10 pages, will break on empty
            url = OR_SEARCH_URL.format(page=page)
            try:
                resp = requests.get(url, headers={
                    "User-Agent": USER_AGENT,
                    "Accept-Encoding": "gzip, deflate",
                }, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                self.logger.warning(f"  Failed to fetch search page {page}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find all links that point to nodes/view/
            items_found = 0
            for link in soup.find_all("a", href=True):
                href = link["href"]
                node_match = NODE_RE.search(href)
                if not node_match:
                    continue

                node_id = int(node_match.group(1))

                # Get alt text from child img, or the link text
                alt_text = ""
                img = link.find("img")
                if img and img.get("alt"):
                    alt_text = img["alt"]
                if not alt_text:
                    alt_text = link.get_text(strip=True)

                if not alt_text:
                    continue

                # Skip YTD / annual items (e.g., "2021 Oregon Lottery ... YTD")
                if "ytd" in alt_text.lower() or "annual" in alt_text.lower():
                    continue

                # Extract YYYY-MM from alt text
                date_match = DATE_RE.search(alt_text)
                if not date_match:
                    continue

                year = int(date_match.group(1))
                month = int(date_match.group(2))

                # Validate date range
                if year < 2019 or year > 2030 or month < 1 or month > 12:
                    continue

                # Deduplicate: keep first node_id per (year, month)
                key = (year, month)
                if key not in seen_months:
                    seen_months[key] = node_id
                    items_found += 1

            # If no items found on this page, we've exhausted results
            if items_found == 0 and page > 1:
                break

        # Build period list
        for (year, month), node_id in sorted(seen_months.items()):
            last_day = calendar.monthrange(year, month)[1]
            periods.append({
                "download_url": OR_PDF_URL.format(node_id=node_id),
                "period_end": date(year, month, last_day),
                "period_type": "monthly",
                "node_id": node_id,
                "year": year,
                "month": month,
            })

        self.logger.info(f"  Discovered {len(periods)} OR monthly periods")
        return sorted(periods, key=lambda p: p["period_end"])

    # ------------------------------------------------------------------
    # download_report
    # ------------------------------------------------------------------
    def download_report(self, period_info: dict) -> Path:
        """Download an Oregon monthly sports betting PDF."""
        url = period_info["download_url"]
        year = period_info["year"]
        month = period_info["month"]
        filename = f"OR_{year}_{month:02d}.pdf"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            return save_path

        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=60)

        if resp.status_code != 200:
            raise FileNotFoundError(
                f"OR PDF not found: {url} (status {resp.status_code})"
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
        Parse Oregon monthly sports betting PDF.

        Extracts:
        - Summary TOTAL row for the aggregate
        - Activity by Sport rows for sport breakdown
        - Combines platforms (DraftKings + Scoreboard) if split

        Returns one aggregate row (sport_category=None) + N sport rows.
        """
        period_end = period_info["period_end"]
        source_url = period_info.get('download_url', period_info.get('url', None))

        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        # Extract text from all pages
        full_text = ""
        n_pages = len(pdf.pages)
        for page in pdf.pages:
            full_text += (page.extract_text() or "") + "\n"
        pdf.close()

        if len(full_text.strip()) < 50:
            self.logger.warning(f"  Empty PDF: {file_path.name}")
            return pd.DataFrame()

        # Parse the Summary TOTAL for aggregate values
        summary_handle, summary_ggr, summary_raw_line = self._parse_summary_total(full_text)

        # Parse Activity by Sport sections (may be split by platform)
        sport_data = self._parse_sport_sections(full_text)

        rows = []

        # Build sport breakdown rows
        for sport_name, (handle, ggr, sport_raw_line) in sport_data.items():
            if handle == 0.0 and ggr == 0.0:
                continue  # Skip zero-activity sports
            row = self._build_row(period_end, handle, ggr, sport_category=sport_name)
            row["source_raw_line"] = sport_raw_line
            rows.append(row)

        # Build aggregate row from Summary TOTAL
        if summary_handle is not None or summary_ggr is not None:
            row = self._build_row(period_end, summary_handle, summary_ggr, sport_category=None)
            row["source_raw_line"] = summary_raw_line
            rows.append(row)
        elif sport_data:
            # If no summary total found, compute from sport totals
            total_handle = sum(h for h, g, _ in sport_data.values())
            total_ggr = sum(g for h, g, _ in sport_data.values())
            rows.append(self._build_row(period_end, total_handle, total_ggr, sport_category=None))

        if not rows:
            self.logger.warning(f"  No data found in {file_path.name}")
            return pd.DataFrame()

        # Add source provenance fields
        for row in rows:
            row["source_file"] = file_path.name
            row["source_page"] = 1 if n_pages == 1 else None
            row["source_url"] = source_url

        df = pd.DataFrame(rows)
        df["period_end"] = pd.to_datetime(df["period_end"])
        df["period_start"] = df["period_end"].apply(lambda d: d.replace(day=1))
        return df

    # ------------------------------------------------------------------
    # _parse_summary_total
    # ------------------------------------------------------------------
    def _parse_summary_total(self, text: str) -> tuple:
        """
        Extract TOTAL Turnover and GGR from the Summary section.

        The Summary section has lines like:
          January 2026   $82,150,853   $11,109,938   13.5%
          TOTAL          $82,150,853   $11,109,938   13.5%

        For split-platform PDFs (2022 era):
          January 2022 - DraftKings   $22,226,870   $1,248,173   5.6%
          January 2022 - Scoreboard   $20,672,865   $1,141,107   5.5%
          TOTAL                       $42,899,735   $2,389,280   5.6%

        We want the TOTAL line from the Summary section (before Activity by Sport).
        """
        lines = text.splitlines()
        in_summary = False
        total_handle = None
        total_ggr = None
        total_raw_line = None

        for line in lines:
            stripped = line.strip()
            lower = stripped.lower()

            # Enter summary section
            if lower == "summary":
                in_summary = True
                continue

            # Exit summary section when we hit Activity or Source
            if in_summary and ("activity by" in lower or lower.startswith("source:")):
                if total_handle is not None:
                    break
                in_summary = False
                continue

            if not in_summary:
                continue

            # Skip header line
            if "period" in lower and "turnover" in lower:
                continue

            # Look for TOTAL line
            if lower.startswith("total"):
                handle, ggr = self._extract_turnover_ggr(stripped)
                if handle is not None:
                    total_handle = handle
                    total_ggr = ggr
                    total_raw_line = stripped
                    break

        return total_handle, total_ggr, total_raw_line

    # ------------------------------------------------------------------
    # _parse_sport_sections
    # ------------------------------------------------------------------
    def _parse_sport_sections(self, text: str) -> dict:
        """
        Parse all Activity by Sport sections and combine across platforms.

        Handles:
        - Single section: "Activity by Sport"
        - Split sections: "Activity by Sport - DraftKings" and "Activity by Sport - Scoreboard"

        Pre-aggregates by the final normalized sport category (using
        normalize_sport from operator_mapping) so that sports like
        "Handball", "Darts", "Volleyball" which all map to "other"
        are combined into a single row before returning.

        Returns dict of raw_sport_name -> (total_handle, total_ggr) where
        raw_sport_name is a canonical name that normalize_sport() will
        correctly map to the desired category.
        """
        from scrapers.operator_mapping import normalize_sport

        lines = text.splitlines()
        # Accumulate by final normalized category -> [handle, ggr]
        norm_data = {}
        # Track raw lines per normalized category
        norm_raw_lines = {}
        in_sport_section = False

        for line in lines:
            stripped = line.strip()
            lower = stripped.lower()

            # Detect start of an Activity by Sport section
            if "activity by sport" in lower:
                in_sport_section = True
                continue

            # Exit sport section on Activity by Bet Type, Source, Terminology, or new section
            if in_sport_section and (
                "activity by bet type" in lower
                or "activity by live" in lower
                or lower.startswith("source:")
                or lower == "terminology"
                or "registrations" in lower
            ):
                in_sport_section = False
                continue

            if not in_sport_section:
                continue

            # Skip headers and empty lines
            if not stripped:
                continue
            if "sport" in lower and "turnover" in lower:
                continue
            if lower.startswith("# unique") or lower.startswith("# of"):
                continue
            # Skip month/date header lines inside section
            if re.match(r'^[A-Za-z]+\s+\d{4}$', stripped):
                continue
            # Skip "Acccessed" / "Accessed" lines
            if lower.startswith("acc"):
                continue

            # Skip the TOTAL line (we handle aggregate from Summary)
            if lower.startswith("total"):
                in_sport_section = False  # TOTAL ends this section
                continue

            # Parse sport row
            sport_name, handle, ggr = self._parse_sport_line(stripped)
            if sport_name is not None and handle is not None:
                # First normalize to our standard name, then get final category
                std_name = self._normalize_sport_name(sport_name)
                final_cat = normalize_sport(std_name) or "other"

                if final_cat not in norm_data:
                    norm_data[final_cat] = [0.0, 0.0]
                    norm_raw_lines[final_cat] = []
                norm_data[final_cat][0] += handle
                if ggr is not None:
                    norm_data[final_cat][1] += ggr
                norm_raw_lines[final_cat].append(stripped)

        # Map normalized categories back to canonical raw names that
        # normalize_sport() will correctly recognize
        _NORM_TO_RAW = {
            "football": "Football",
            "basketball": "Basketball",
            "baseball": "Baseball",
            "hockey": "Ice Hockey",
            "soccer": "Soccer",
            "tennis": "Tennis",
            "table_tennis": "Table Tennis",
            "golf": "Golf",
            "boxing": "Boxing",
            "mma": "MMA",
            "motorsports": "Motor Racing",
            "cricket": "Cricket",
            "rugby": "Rugby",
            "esports": "Esports",
            "lacrosse": "Lacrosse",
            "other": "Other",
        }

        result = {}
        for norm_cat, (h, g) in norm_data.items():
            raw_name = _NORM_TO_RAW.get(norm_cat, norm_cat)
            raw_lines = norm_raw_lines.get(norm_cat, [])
            result[raw_name] = (h, g, ' | '.join(raw_lines) if raw_lines else None)

        return result

    # ------------------------------------------------------------------
    # _parse_sport_line
    # ------------------------------------------------------------------
    def _parse_sport_line(self, line: str) -> tuple:
        """
        Parse a single sport line from the Activity by Sport table.

        Formats encountered:
          Baseball         $22,507      ($1,458)     -6.5%
          BasketBall       $31,910,342  $5,260,741   16.5%
          Table Tennis     $8,568,132   $708,072     8.3%

        Older format with extra columns:
          BasketBall  430,183  $12,349,411  $730,137  5.91%  14,251  $28.71

        Returns (sport_name, handle, ggr) or (None, None, None) on failure.
        """
        # Find all dollar values (positive and negative/parenthesized)
        money_pattern = re.compile(r'\(\$[\d,]+(?:\.\d+)?\)|\$[\d,]+(?:\.\d+)?')
        money_strs = money_pattern.findall(line)

        if len(money_strs) < 2:
            return None, None, None

        # Extract sport name: everything before the first $ or ($
        # But also handle lines with bare numbers before dollar amounts (older format)
        sport_match = re.match(r'^(.+?)(?:\s+[\d,]+\s+)?\s*(?:\$|\(\$)', line)
        if sport_match:
            sport_name = sport_match.group(1).strip()
        else:
            return None, None, None

        # Clean up sport name — remove trailing digits/commas from older format
        sport_name = re.sub(r'\s+\d[\d,]*$', '', sport_name).strip()

        if not sport_name:
            return None, None, None

        # First dollar value = Turnover (handle)
        # Second dollar value = GGR
        handle = clean_currency(money_strs[0])
        ggr = clean_currency(money_strs[1])

        return sport_name, handle, ggr

    # ------------------------------------------------------------------
    # _extract_turnover_ggr
    # ------------------------------------------------------------------
    def _extract_turnover_ggr(self, line: str) -> tuple:
        """
        Extract Turnover and GGR from a line containing dollar values.

        Returns (handle, ggr) or (None, None) on failure.
        """
        money_pattern = re.compile(r'\(\$[\d,]+(?:\.\d+)?\)|\$[\d,]+(?:\.\d+)?')
        money_strs = money_pattern.findall(line)

        if len(money_strs) < 2:
            return None, None

        handle = clean_currency(money_strs[0])
        ggr = clean_currency(money_strs[1])
        return handle, ggr

    # ------------------------------------------------------------------
    # _normalize_sport_name
    # ------------------------------------------------------------------
    def _normalize_sport_name(self, raw_name: str) -> str:
        """
        Normalize sport names to standard categories for combining across
        platforms and eras.

        Maps raw PDF names to standard names that the base scraper's
        normalize_sport() will recognize.
        """
        SPORT_NORM = {
            "baseball": "Baseball",
            "basketball": "Basketball",
            "football": "Football",
            "ice hockey": "Ice Hockey",
            "hockey": "Ice Hockey",
            "soccer": "Soccer",
            "tennis": "Tennis",
            "table tennis": "Table Tennis",
            "golf": "Golf",
            "boxing": "Boxing",
            "mma": "MMA",
            "motor racing": "Motor Racing",
            "motorsports": "Motor Racing",
            "cricket": "Cricket",
            "darts": "Darts",
            "rugby league": "Rugby",
            "rugby union": "Rugby",
            "rugby": "Rugby",
            "volleyball": "Volleyball",
            "cycling": "Cycling",
            "handball": "Handball",
            "snooker & pool": "Other",
            "snooker": "Other",
            "scoreboard specials": "Other",
            "aussie rules": "Other",
            "bandy": "Other",
            "esports": "Esports",
            "lacrosse": "Lacrosse",
            "other": "Other",
        }
        lower = raw_name.lower().strip()
        return SPORT_NORM.get(lower, raw_name)

    # ------------------------------------------------------------------
    # _build_row
    # ------------------------------------------------------------------
    def _build_row(
        self,
        period_end: date,
        handle: float | None,
        ggr: float | None,
        sport_category: str | None,
    ) -> dict:
        """
        Build a standardized data row.

        - handle = Turnover (total wagers)
        - ggr = Gross Gaming Revenue = Turnover - Payouts (can be negative)
        - payouts = handle - ggr (derived)
        - standard_ggr = ggr (GGR is already handle - payouts by definition)
        - channel = "online" (Oregon is online-only)
        - operator_raw = "DraftKings" (sole operator)
        """
        row = {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "DraftKings",
            "channel": "online",
            "handle": handle,
        }

        if ggr is not None:
            row["gross_revenue"] = ggr
            row["standard_ggr"] = ggr

            if handle is not None:
                row["payouts"] = handle - ggr

            # Hold percentage
            if handle is not None and handle != 0:
                row["hold_pct"] = ggr / handle

        if sport_category is not None:
            row["sport_category"] = sport_category

        return row


if __name__ == "__main__":
    scraper = ORScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"OR SCRAPER RESULTS")
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
        # Show a few sample rows
        print(f"\nSample aggregate rows:")
        for _, row in agg.tail(3).iterrows():
            h = row.get('handle', 'N/A')
            g = row.get('gross_revenue', 'N/A')
            if pd.notna(h):
                h = f"${h/100:,.0f}"
            if pd.notna(g):
                g = f"${g/100:,.0f}"
            print(f"  {row['period_end'].strftime('%Y-%m')}: handle={h}, GGR={g}")

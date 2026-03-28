"""
Montana Sports Betting Revenue Scraper
Source: Sports Bet Montana weekly/monthly activity PDFs
Format: PDF (weekly and monthly activity reports from sportsbetmontana.com)
Launch: March 2020
Tax: Lottery retains proceeds (revenue share model, ~8.5% effective rate)
Operator: Single operator - Montana Lottery / Intralot ("Sports Bet Montana")
Very small market (~$7M GGR annually), retail only (Intralot terminals in bars/restaurants)

Data source: sportsbetmontana.com/en/view/news
Monthly PDFs: Sports_Bet_Montana_Activity_{Month}_{Year}.pdf
Weekly PDFs:  Sports_Bet_Montana_Activity_WE_{M}.{D}.{YY}.pdf

PDF structure:
  - Project To Date summary (left side) — cumulative since launch, SKIP these
  - Monthly/Weekly Activity summary (right side) — period values, USE these
  - Sport detail table: per-sport Handle, Payout, GGR
"""

import sys
import re
import calendar
from pathlib import Path
from datetime import date, datetime, timedelta

import pandas as pd
import pdfplumber
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, clean_currency

MT_NEWS_URL = "https://sportsbetmontana.com/en/view/news"
MT_BASE_URL = "https://sportsbetmontana.com"

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MONTH_NAME_TO_NUM = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Sport names from the PDF mapped to our standard names.
# normalize_sport in operator_mapping handles these already, but we
# explicitly map here for clarity and to catch edge cases.
SPORT_NAME_MAP = {
    "football": "Football",
    "basketball": "Basketball",
    "ice hockey": "Ice Hockey",
    "hockey": "Ice Hockey",
    "soccer": "Soccer",
    "mma": "MMA",
    "specials": "Specials",
    "tennis": "Tennis",
    "table tennis": "Table Tennis",
    "golf": "Golf",
    "baseball": "Baseball",
    "other": "Other",
}


class MTScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("MT")

    # ------------------------------------------------------------------
    # discover_periods
    # ------------------------------------------------------------------
    def discover_periods(self) -> list[dict]:
        """
        Fetch the Sports Bet Montana news page and extract all monthly
        and weekly PDF links.

        Monthly filename pattern:
            Sports_Bet_Montana_Activity_{Month}_{Year}.pdf
        Weekly filename pattern:
            Sports_Bet_Montana_Activity_WE_{M}.{D}.{YY}.pdf
        """
        periods = []
        seen = set()

        try:
            resp = requests.get(MT_NEWS_URL, headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
            }, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to fetch news page: {e}")
            return periods

        # The news page embeds PDF links inside escaped JSON/HTML (the <a> tags
        # are inside a JS-rendered CMS payload with deeply escaped slashes like
        # \\/static\\/assets\\/Sports_Bet_Montana_Activity_WE_1.4.25.pdf).
        # BeautifulSoup cannot parse these. Instead, extract unique PDF filenames
        # directly from the raw HTML text using regex.
        raw_html = resp.text

        # Find all unique PDF filenames matching our patterns
        pdf_filenames = set(re.findall(
            r'(Sports_Bet_Montana_Activity[^"\\<> ]+\.pdf)',
            raw_html
        ))

        for filename in pdf_filenames:
            full_url = f"{MT_BASE_URL}/static/assets/{filename}"

            # Try monthly pattern: Sports_Bet_Montana_Activity_{Month}_{Year}.pdf
            monthly_match = re.search(
                r'Sports_Bet_Montana_Activity_(\w+)_(\d{4})\.pdf',
                filename, re.IGNORECASE
            )
            if monthly_match:
                month_name = monthly_match.group(1).lower()
                year = int(monthly_match.group(2))
                # Skip "WE" — that's a weekly prefix, not a month
                if month_name == "we":
                    continue
                month_num = MONTH_NAME_TO_NUM.get(month_name)
                if month_num and 2020 <= year <= 2030:
                    last_day = calendar.monthrange(year, month_num)[1]
                    period_end = date(year, month_num, last_day)
                    key = ("monthly", period_end)
                    if key not in seen:
                        seen.add(key)
                        periods.append({
                            "download_url": full_url,
                            "period_end": period_end,
                            "period_type": "monthly",
                        })
                continue

            # Try weekly pattern: Sports_Bet_Montana_Activity_WE_{M}.{D}.{YY}.pdf
            weekly_match = re.search(
                r'Sports_Bet_Montana_Activity_WE_(\d{1,2})\.(\d{1,2})\.(\d{2,4})\.pdf',
                filename, re.IGNORECASE
            )
            if weekly_match:
                m = int(weekly_match.group(1))
                d = int(weekly_match.group(2))
                yy = int(weekly_match.group(3))
                # Handle 2-digit year
                if yy < 100:
                    year = 2000 + yy
                else:
                    year = yy
                if 1 <= m <= 12 and 1 <= d <= 31 and 2020 <= year <= 2030:
                    try:
                        period_end = date(year, m, d)
                        key = ("weekly", period_end)
                        if key not in seen:
                            seen.add(key)
                            periods.append({
                                "download_url": full_url,
                                "period_end": period_end,
                                "period_type": "weekly",
                            })
                    except ValueError:
                        self.logger.warning(
                            f"Invalid date in weekly PDF: {filename}"
                        )

        monthly_count = sum(1 for p in periods if p["period_type"] == "monthly")
        weekly_count = sum(1 for p in periods if p["period_type"] == "weekly")
        self.logger.info(
            f"  Discovered {len(periods)} MT periods "
            f"({monthly_count} monthly, {weekly_count} weekly)"
        )
        return sorted(periods, key=lambda p: (p["period_end"], p["period_type"]))

    # ------------------------------------------------------------------
    # download_report
    # ------------------------------------------------------------------
    def download_report(self, period_info: dict) -> Path:
        """Download Sports Bet Montana activity PDF."""
        url = period_info["download_url"]
        period_end = period_info["period_end"]
        period_type = period_info["period_type"]

        if period_type == "weekly":
            filename = f"MT_weekly_{period_end.isoformat()}.pdf"
        else:
            filename = f"MT_monthly_{period_end.year}_{period_end.month:02d}.pdf"

        save_path = self.raw_dir / filename

        if save_path.exists() and save_path.stat().st_size > 1000:
            return save_path

        resp = requests.get(url, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=60)

        if resp.status_code != 200:
            raise FileNotFoundError(
                f"MT PDF not found: {url} (status {resp.status_code})"
            )

        # Verify it's actually a PDF
        if not resp.content[:5].startswith(b'%PDF'):
            raise ValueError(f"Response is not a PDF: {url}")

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
        Parse Sports Bet Montana activity PDF.

        Extracts:
        1. Summary values (Handle, Payout, GGR) from the right-side
           "Monthly/Weekly Activity" section (NOT the left-side "Project To Date")
        2. Per-sport breakdown from the detail table

        Returns 1 aggregate row + N sport rows, all channel="retail".
        """
        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        period_end = period_info["period_end"]
        period_type = period_info["period_type"]

        # Extract all text from the PDF
        full_text = ""
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            full_text += page_text + "\n"
        pdf.close()

        if not full_text.strip():
            self.logger.warning(f"Empty PDF: {file_path.name}")
            return pd.DataFrame()

        rows = []

        # --- Parse summary values ---
        handle, payouts, ggr = self._parse_summary_values(full_text)

        if handle is None:
            self.logger.warning(
                f"Could not parse summary values from {file_path.name}"
            )
            return pd.DataFrame()

        # Build source_raw_line from the summary lines
        summary_parts = []
        if handle is not None:
            summary_parts.append(f"Handle: {handle}")
        if payouts is not None:
            summary_parts.append(f"Payout: {payouts}")
        if ggr is not None:
            summary_parts.append(f"GGR: {ggr}")

        # Aggregate row
        agg_row = {
            "period_end": period_end,
            "period_type": period_type,
            "operator_raw": "Sports Bet Montana",
            "channel": "retail",
            "handle": handle,
            "source_raw_line": ' | '.join(summary_parts),
        }
        if payouts is not None:
            agg_row["payouts"] = payouts
        if ggr is not None:
            agg_row["gross_revenue"] = ggr
            agg_row["standard_ggr"] = ggr
        rows.append(agg_row)

        # --- Parse sport detail table ---
        sport_rows = self._parse_sport_table(full_text, period_end, period_type)
        if sport_rows:
            rows.extend(sport_rows)

        result = pd.DataFrame(rows)

        # Source provenance fields
        result["source_file"] = file_path.name
        result["source_url"] = period_info.get('download_url', period_info.get('url', None))
        result["source_page"] = 1  # Single-page PDF

        # Set period_start based on period_type
        if period_type == "weekly":
            result["period_start"] = period_end - timedelta(days=6)
        else:
            result["period_start"] = period_end.replace(day=1)

        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = pd.to_datetime(result["period_start"])

        return result

    # ------------------------------------------------------------------
    # _parse_summary_values — extract Handle, Payout, GGR from right side
    # ------------------------------------------------------------------
    def _parse_summary_values(self, text: str):
        """
        Extract the Monthly/Weekly Activity summary values.

        The PDF has two columns of data on the same lines:
          - Left: "Project To Date" with abbreviated values like "$321.65M"
          - Right: "Monthly/Weekly Activity" with full values like "$7,884,294"

        Strategy: For each summary line (Handle, Payout, GGR), find all
        dollar amounts and take the one that does NOT have an M/B suffix
        (the full-format monthly/weekly value). If both are full format,
        take the second one (right-side).
        """
        handle = None
        payouts = None
        ggr = None

        lines = text.splitlines()
        for line in lines:
            stripped = line.strip()

            # Match lines with Handle:, Payout:, or GGR:
            if "Handle:" in stripped:
                handle = self._extract_period_value(stripped)
            elif "Payout:" in stripped:
                payouts = self._extract_period_value(stripped)
            elif "GGR:" in stripped:
                ggr = self._extract_period_value(stripped)

        return handle, payouts, ggr

    def _extract_period_value(self, line: str) -> float | None:
        """
        From a line like:
          "Handle: $321.65M  Handle: $7,884,294"
        extract the right-side (non-abbreviated) value.

        Strategy:
        1. Find all dollar amounts in the line
        2. Filter out those ending in M or B (abbreviated project-to-date values)
        3. If multiple remain, take the last one (rightmost)
        4. If none remain without M/B suffix, try parsing all and take the
           smaller one (period value is always smaller than cumulative)
        """
        # Find all dollar-like patterns, including negative in parens
        # Matches: $321.65M, $7,884,294, ($4,510), $12.108M, etc.
        dollar_pattern = re.findall(
            r'[\(\-]?\$[\d,]+(?:\.\d+)?[MB]?\)?',
            line
        )

        if not dollar_pattern:
            return None

        # Separate abbreviated (ending in M or B) from full values
        full_values = []
        abbrev_values = []

        for d in dollar_pattern:
            clean = d.strip()
            if clean.rstrip(')').upper().endswith('M') or clean.rstrip(')').upper().endswith('B'):
                abbrev_values.append(clean)
            else:
                full_values.append(clean)

        if full_values:
            # Take the last full-format value (rightmost = monthly/weekly)
            raw = full_values[-1]
            return self._parse_dollar_value(raw)
        elif abbrev_values:
            # Fallback: all values are abbreviated; shouldn't normally happen
            # for the monthly/weekly section but handle gracefully
            self.logger.warning(
                f"Only abbreviated values found in line: {line}"
            )
            return None

        return None

    def _parse_dollar_value(self, raw: str) -> float | None:
        """
        Parse a dollar string like "$7,884,294" or "($4,510)" into a float.
        Handles parenthesized negatives.
        """
        s = raw.strip()

        # Check for negative (parenthesized)
        is_negative = False
        if s.startswith('(') and s.endswith(')'):
            is_negative = True
            s = s[1:-1]
        elif s.startswith('-'):
            is_negative = True
            s = s[1:]

        # Remove $ and commas
        s = s.replace('$', '').replace(',', '').strip()

        if not s:
            return None

        try:
            val = float(s)
            return -val if is_negative else val
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # _parse_sport_table — extract per-sport rows
    # ------------------------------------------------------------------
    def _parse_sport_table(self, text: str, period_end: date,
                           period_type: str) -> list[dict]:
        """
        Parse the sport detail table from the PDF text.

        Expected format (after text extraction):
            Sport         Handle ($)  Handle (%)  Payout ($)  GGR ($)
            Football      $3,780,247  47.9%       $3,667,630  $112,617
            Basketball    $2,895,429  36.7%       $2,564,268  $331,161
            ...
            Total         $7,884,294              $7,235,609  $648,685

        We look for lines starting with known sport names followed by dollar amounts.
        Skip the "Total" row.
        """
        rows = []
        lines = text.splitlines()

        # Known sport names (lowercase) that appear in the table
        known_sports = {
            "football", "basketball", "ice hockey", "soccer", "mma",
            "specials", "tennis", "table tennis", "golf", "baseball",
            "other", "hockey", "boxing", "motorsports", "cricket",
            "rugby", "lacrosse", "esports",
        }

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Skip header and total lines
            stripped_lower = stripped.lower()
            if stripped_lower.startswith("sport") or stripped_lower.startswith("total"):
                continue

            # Try to match a sport row: sport name followed by dollar amounts
            # The sport name may be one or two words (e.g., "Ice Hockey", "Table Tennis")
            sport_match = re.match(
                r'^([A-Za-z][A-Za-z ]+?)\s+'       # Sport name (1+ words)
                r'[\(\$\-]',                        # Followed by $ or ( or -
                stripped
            )
            if not sport_match:
                continue

            sport_name = sport_match.group(1).strip()

            # Verify it's a known sport
            if sport_name.lower() not in known_sports:
                continue

            # Extract all dollar amounts from the line
            dollar_amounts = re.findall(
                r'[\(\-]?\$[\d,]+(?:\.\d+)?\)?',
                stripped
            )

            if len(dollar_amounts) < 2:
                continue

            # Parse the amounts:
            # Column order: Handle ($), Handle (%), Payout ($), GGR ($)
            # dollar_amounts should be [handle, payout, ggr] (% is not a dollar amount)
            sport_handle = self._parse_dollar_value(dollar_amounts[0])
            sport_payout = self._parse_dollar_value(dollar_amounts[1]) if len(dollar_amounts) >= 2 else None
            sport_ggr = self._parse_dollar_value(dollar_amounts[2]) if len(dollar_amounts) >= 3 else None

            # Determine correct column assignment:
            # The Handle (%) column is not a dollar amount, so dollar_amounts should be:
            # [Handle ($), Payout ($), GGR ($)]
            # But sometimes the percentage appears without $ sign, so it's fine.

            if sport_handle is None:
                continue

            # Map the sport name to the standard format that normalize_sport expects
            display_name = SPORT_NAME_MAP.get(sport_name.lower(), sport_name)

            row = {
                "period_end": period_end,
                "period_type": period_type,
                "operator_raw": "Sports Bet Montana",
                "channel": "retail",
                "sport_category": display_name,
                "handle": sport_handle,
                "source_raw_line": stripped,
            }
            if sport_payout is not None:
                row["payouts"] = sport_payout
            if sport_ggr is not None:
                row["gross_revenue"] = sport_ggr
                row["standard_ggr"] = sport_ggr

            rows.append(row)

        if rows:
            self.logger.debug(
                f"  Parsed {len(rows)} sport rows for {period_end}"
            )

        return rows


if __name__ == "__main__":
    scraper = MTScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"MT SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        if 'period_type' in df.columns:
            print(f"Period types: {df['period_type'].value_counts().to_dict()}")
        if 'sport_category' in df.columns:
            sports = df[df['sport_category'].notna()]['sport_category'].unique()
            print(f"Sports found: {sorted(sports)}")
        # Show a sample of monthly aggregate rows
        monthly_agg = df[
            (df['period_type'] == 'monthly') &
            (df['sport_category'].isna())
        ].sort_values('period_end')
        if not monthly_agg.empty:
            print(f"\nMonthly aggregate rows ({len(monthly_agg)}):")
            for _, row in monthly_agg.tail(3).iterrows():
                h = row.get('handle')
                p = row.get('payouts')
                g = row.get('gross_revenue')
                # Values are in cents after normalization
                h_str = f"${h/100:,.0f}" if pd.notna(h) else "N/A"
                p_str = f"${p/100:,.0f}" if pd.notna(p) else "N/A"
                g_str = f"${g/100:,.0f}" if pd.notna(g) else "N/A"
                print(f"  {row['period_end'].date()}: handle={h_str}, payouts={p_str}, GGR={g_str}")

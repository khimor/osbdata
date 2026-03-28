"""
Virginia Sports Betting Scraper
Source: VA Lottery monthly press releases via valottery.com API
Format: JSON API returning HTML tables in BodyHtml
Launch: January 2021
Tax: 15% on adjusted gross revenue
Note: VA Lottery publishes monthly press releases with financial tables.
      POST to /api/v1/latestwinners with keyword="sports wagering activity".
      Early reports (Jan-Apr 2021) are text-only; May 2021+ have HTML tables.
      Casino Retail column appears starting Jan 2023.
"""

import sys
import re
import json
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, clean_currency

API_URL = "https://www.valottery.com/api/v1/latestwinners"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Row label -> our field name mapping
ROW_MAPPING = {
    "gross sports gaming revenues": "handle",
    "gross winnings": "payouts",
    "allowable bonuses and promotions": "promo_credits",
    "bonuses and promotions": "promo_credits",
    "agr (adjusted gross revenue)": "net_revenue",
    "agr(adjusted gross revenue)": "net_revenue",
}


class VAScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("VA")

    def discover_periods(self) -> list[dict]:
        """Discover VA sports betting reports from VA Lottery press releases API."""
        periods = []
        seen = set()

        try:
            resp = requests.post(API_URL, data={
                "keyword": "sports wagering activity",
                "year": "",
                "page": "0",
                "pageSize": "100",
                "specificYear": "0",
                "specificWinner": "",
                "QueryString": "",
            }, headers={"User-Agent": USER_AGENT}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch VA Lottery API: {e}")
            return []

        items = data.get("data", [])
        self.logger.info(f"  API returned {len(items)} results")

        for item in items:
            title = item.get("Title", "")
            pub_date_str = item.get("PublicationDate", "")
            body_html = item.get("BodyHtml", "")
            item_id = item.get("Id", "")

            # Extract data month/year from title, body text, and table headers
            month_num, year = self._extract_data_month_year(
                title, body_html, pub_date_str
            )

            if month_num is None or year is None:
                self.logger.warning(
                    f"  Could not extract month/year from: {title[:80]}"
                )
                continue

            key = (year, month_num)
            if key in seen:
                continue
            seen.add(key)

            last_day = calendar.monthrange(year, month_num)[1]
            period_end = date(year, month_num, last_day)

            periods.append({
                "period_end": period_end,
                "period_type": "monthly",
                "item_id": item_id,
                "title": title,
                "body_html": body_html,
                "pub_date": pub_date_str,
            })

        self.logger.info(f"  Found {len(periods)} VA monthly periods")
        return periods

    def _extract_data_month_year(
        self, title: str, body_html: str, pub_date_str: str
    ) -> tuple:
        """Extract the data month and year from title, body HTML, and publication date.

        Returns (month_num, year) or (None, None) if unable to determine.
        """
        title_lower = title.lower()
        pub_date = None
        if pub_date_str:
            try:
                pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        # Strategy 1: Look for month name in title using the standard pattern
        # "releases {Month} sports wagering" to avoid matching months in other
        # parts of the title (e.g., "45% increase over September")
        found_month = None
        release_match = re.search(
            r'releases?\s+(\w+)\s+sports\s+wagering', title_lower
        )
        if release_match:
            m_name = release_match.group(1)
            found_month = MONTH_NAMES.get(m_name)

        # Fallback: "releases first monthly ... for the month of {Month}"
        if found_month is None:
            first_match = re.search(
                r'for the month of (\w+)', title_lower
            )
            if first_match:
                found_month = MONTH_NAMES.get(first_match.group(1))

        if found_month is not None and pub_date is not None:
            # The report about month X is published 1-3 months later.
            # Use publication date to determine the year.
            pub_year = pub_date.year
            pub_month = pub_date.month

            # Data month is before the publication month
            # If pub_month <= found_month, data is from the previous year
            if pub_month <= found_month and found_month - pub_month > 3:
                year = pub_year - 1
            elif pub_month > found_month:
                year = pub_year
            elif pub_month <= found_month:
                # Same year (e.g., pub March for January report)
                year = pub_year
            else:
                year = pub_year

            # Sanity: the data month shouldn't be after the pub date
            if date(year, found_month, 1) > pub_date.date():
                year -= 1

            return found_month, year

        # Strategy 2: Check table header for month abbreviation (e.g., "May 2021", "Aug-21")
        if body_html:
            soup = BeautifulSoup(body_html, "html.parser")
            tables = soup.find_all("table")
            if tables:
                header_row = tables[0].find("tr")
                if header_row:
                    header_text = header_row.get_text(separator=" ").lower()
                    for month_name, month_num in MONTH_NAMES.items():
                        short = month_name[:3]
                        if short in header_text:
                            # Try to find year
                            year_match = re.search(
                                rf'{short}\w*[\s.-]+(\d{{2,4}})', header_text
                            )
                            if year_match:
                                yr = int(year_match.group(1))
                                if yr < 100:
                                    yr += 2000
                                return month_num, yr

            # Strategy 3: Parse from body text "for the month of {Month}"
            body_text = soup.get_text(separator=" ").lower()
            month_match = re.search(
                r'for the month of (\w+)', body_text
            )
            if month_match:
                m_name = month_match.group(1).strip()
                for month_name, month_num in MONTH_NAMES.items():
                    if m_name == month_name or m_name == month_name[:3]:
                        if pub_date:
                            year = pub_date.year
                            if date(year, month_num, 1) > pub_date.date():
                                year -= 1
                            return month_num, year

        return None, None

    def download_report(self, period_info: dict) -> Path:
        """Save the BodyHtml as an HTML file for the period."""
        period_end = period_info["period_end"]
        filename = f"VA_{period_end.year}_{period_end.month:02d}.html"
        save_path = self.raw_dir / filename

        body_html = period_info.get("body_html", "")
        if not body_html:
            raise FileNotFoundError(
                f"No BodyHtml for VA {period_end.year}-{period_end.month:02d}"
            )

        with open(save_path, "w", encoding="utf-8") as f:
            f.write(body_html)

        self.logger.info(
            f"  Saved: {filename} ({save_path.stat().st_size:,} bytes)"
        )
        return save_path

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse VA sports wagering press release HTML.

        Two formats:
        1. Table-based (May 2021+): HTML table with financial rows
        2. Text-based (Jan-Apr 2021): Numbers embedded in prose
        """
        period_end = period_info["period_end"]

        # Source provenance — VA data comes from JSON API
        source_file = API_URL
        source_url = period_info.get('download_url', period_info.get('url', None))

        with open(file_path, "r", encoding="utf-8") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")

        if tables:
            rows = self._parse_table(tables, period_end)
            # Capture raw table HTML as source_raw_line
            raw_table_text = tables[0].get_text(separator=" | ", strip=True)
            # Build source_context from table rows
            all_trs = tables[0].find_all('tr')
            header_cells = [th.get_text(strip=True) for th in all_trs[0].find_all(['th', 'td'])] if all_trs else []
            ctx_rows = []
            for j in range(1, len(all_trs)):
                ctx_cells = [td.get_text(strip=True) for td in all_trs[j].find_all(['th', 'td'])]
                ctx_rows.append(ctx_cells[:10])
            source_context = json.dumps({"headers": header_cells[:10], "rows": ctx_rows, "highlight": None})
            for row in rows:
                row["source_raw_line"] = raw_table_text
                row["source_context"] = source_context
        else:
            rows = self._parse_text(soup, period_end)
            # Capture raw body text as source_raw_line
            raw_text = soup.get_text(separator=" ", strip=True)
            for row in rows:
                row["source_raw_line"] = raw_text

        if not rows:
            return pd.DataFrame()

        # Derive standard_ggr (handle - payouts) and gross_revenue for each row
        # VA's state-reported metric is AGR (Adjusted Gross Revenue, after promos)
        # which is stored in net_revenue. gross_revenue should match what the state
        # reports; standard_ggr is the standardized handle - payouts.
        for row in rows:
            h = row.get("handle")
            p = row.get("payouts")
            if h is not None and p is not None:
                row["standard_ggr"] = h - p
            # gross_revenue = state's reported AGR (after promos)
            row["gross_revenue"] = row.get("net_revenue")

        # Add provenance fields to every row
        for row in rows:
            row["source_file"] = source_file
            row["source_url"] = source_url

        result = pd.DataFrame(rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(
            lambda d: d.replace(day=1)
        )
        return result

    def _parse_table(self, tables: list, period_end: date) -> list[dict]:
        """Parse financial data from the HTML table.

        Returns rows for online and/or retail channels.
        """
        table = tables[0]  # First table has financial data

        # Determine columns from header row
        header_row = table.find("tr")
        if not header_row:
            return []

        header_cells = header_row.find_all(["th", "td"])
        headers = [c.get_text(strip=True).lower() for c in header_cells]

        # Identify column indices
        # Formats seen:
        #   ['', 'Mobile Operators', 'Casino Retail Activity', 'Total Jan. 2026']
        #   ['', 'June 2021', 'Inception to Date']
        #   ['', 'Jul-22']
        #   ['', 'Nov-22']
        mobile_col = None
        retail_col = None
        total_col = None
        monthly_col = None  # Column with this month's data (not cumulative)

        month_abbrevs = [
            "jan", "feb", "mar", "apr", "may", "jun",
            "jul", "aug", "sep", "oct", "nov", "dec"
        ]
        cumulative_keywords = ["inception", "launch to date", "year to date",
                               "ytd", "fytd", "cumulative"]

        for i, h in enumerate(headers):
            if "mobile" in h:
                mobile_col = i
            elif "casino" in h or "retail" in h:
                retail_col = i
            elif any(kw in h for kw in cumulative_keywords):
                # Skip cumulative columns entirely
                continue
            elif "total" in h and any(m in h for m in month_abbrevs):
                # "Total Jan. 2026" — this is the combined total column
                total_col = i
            elif any(m in h for m in month_abbrevs):
                # Monthly data column like "June 2021", "Jul-22", "Nov-22"
                monthly_col = i

        # For single-column or two-column (monthly + inception) format,
        # the monthly column is our data source
        if mobile_col is None and monthly_col is not None:
            total_col = monthly_col

        # Parse data rows
        data = {}  # field_name -> {mobile: val, retail: val, total: val}
        data_rows = table.find_all("tr")[1:]  # Skip header

        for row in data_rows:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue

            label = cells[0].get_text(strip=True).lower()

            # Match to our field mapping
            field = None
            for pattern, field_name in ROW_MAPPING.items():
                if pattern in label:
                    field = field_name
                    break

            if field is None:
                continue

            cell_texts = [c.get_text(strip=True) for c in cells]

            values = {}
            if mobile_col is not None and mobile_col < len(cell_texts):
                values["mobile"] = self._parse_money(cell_texts[mobile_col])
            if retail_col is not None and retail_col < len(cell_texts):
                values["retail"] = self._parse_money(cell_texts[retail_col])
            if total_col is not None and total_col < len(cell_texts):
                values["total"] = self._parse_money(cell_texts[total_col])

            data[field] = values

        # Also parse tax from second table if present
        tax_paid = None
        if len(tables) > 1:
            tax_table = tables[1]
            for row in tax_table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    if "total tax" in label:
                        tax_paid = self._parse_money(cells[1].get_text(strip=True))
                        break

        # Extract hold percentage from text body
        hold_pct = self._extract_hold_pct(
            tables[0].find_parent().get_text(separator=" ")
            if tables[0].find_parent() else ""
        )

        # Build output rows
        rows = []
        has_channels = mobile_col is not None

        if has_channels:
            # Separate online and retail rows
            for channel, col_key in [("online", "mobile"), ("retail", "retail")]:
                if col_key == "retail" and retail_col is None:
                    continue

                row = {
                    "period_end": period_end,
                    "period_type": "monthly",
                    "operator_raw": "ALL",
                    "channel": channel,
                }

                for field in ["handle", "payouts", "promo_credits", "net_revenue"]:
                    if field in data and col_key in data[field]:
                        val = data[field][col_key]
                        # payouts and promo_credits are shown as negatives
                        # in parentheses. Make payouts positive (amount paid
                        # out to bettors) and promo_credits positive (credits
                        # given to bettors).
                        if field in ("payouts", "promo_credits") and val is not None:
                            val = abs(val)
                        row[field] = val
                    else:
                        row[field] = None

                # Only add row if there's meaningful data
                if row.get("handle") is not None:
                    if hold_pct is not None:
                        row["hold_pct"] = hold_pct
                    rows.append(row)
        else:
            # Single column (pre-casino era): use total column as combined
            # But we know these are all online (no casinos yet)
            row = {
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": "ALL",
                "channel": "online",
            }

            for field in ["handle", "payouts", "promo_credits", "net_revenue"]:
                if field in data and "total" in data[field]:
                    val = data[field]["total"]
                    if field in ("payouts", "promo_credits") and val is not None:
                        val = abs(val)
                    row[field] = val
                else:
                    row[field] = None

            if row.get("handle") is not None:
                if hold_pct is not None:
                    row["hold_pct"] = hold_pct
                rows.append(row)

        # Apportion tax to channels proportionally based on net_revenue
        if tax_paid is not None and len(rows) > 0:
            total_nr = sum(
                r.get("net_revenue", 0) or 0 for r in rows
            )
            if total_nr > 0:
                for r in rows:
                    nr = r.get("net_revenue", 0) or 0
                    r["tax_paid"] = tax_paid * (nr / total_nr) if nr else 0.0
            elif len(rows) == 1:
                rows[0]["tax_paid"] = tax_paid

        return rows

    def _parse_text(self, soup: BeautifulSoup, period_end: date) -> list[dict]:
        """Parse financial data from text-only reports (Jan-Apr 2021).

        These early reports have no table; data is in prose like:
        "wagered $58,896,564 ... won $55,310,487 ... 6.08% win percentage"
        """
        text = soup.get_text(separator=" ")

        # Extract handle
        handle = None
        handle_match = re.search(r'wagered\s+\$([\d,]+)', text)
        if handle_match:
            handle = self._parse_money("$" + handle_match.group(1))

        # Extract payouts (winnings)
        payouts = None
        won_match = re.search(r'won\s+\$([\d,]+)', text)
        if won_match:
            payouts = self._parse_money("$" + won_match.group(1))

        # Extract hold percentage
        hold_pct = self._extract_hold_pct(text)

        if handle is None:
            return []

        row = {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "ALL",
            "channel": "online",  # Pre-casino era: all online
            "handle": handle,
            "payouts": payouts,
        }

        if hold_pct is not None:
            row["hold_pct"] = hold_pct

        return [row]

    def _extract_hold_pct(self, text: str) -> float | None:
        """Extract hold percentage from text body."""
        if not text:
            return None

        # Patterns: "6.08% win percentage", "8.22% operators win percentage",
        # "6.08% hold"
        patterns = [
            r'(\d+\.?\d*)\s*%\s*(?:operators?\s+)?win\s+percent',
            r'(\d+\.?\d*)\s*%\s*hold',
            r'hold\s+(?:of|was|is)\s+(\d+\.?\d*)\s*%',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1)) / 100.0
                except (ValueError, IndexError):
                    pass

        return None

    def _parse_money(self, value) -> float | None:
        """Parse money value from VA reports.

        Handles formats like: $683,940,659, ($604,749,566), $0
        Returns value in dollars (float). Parenthesized values are negative.
        """
        if value is None:
            return None
        s = str(value).strip()
        if not s or s in ("-", "N/A", "", "$0"):
            if s == "$0":
                return 0.0
            return None

        return clean_currency(s)


if __name__ == "__main__":
    scraper = VAScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"VA SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        cols = ['period_end', 'channel', 'handle', 'payouts', 'gross_revenue',
                'promo_credits', 'net_revenue', 'tax_paid']
        existing = [c for c in cols if c in df.columns]
        print(f"\nSample rows:")
        print(df[existing].head(10).to_string(index=False))

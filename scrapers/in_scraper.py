"""
Indiana Sports Wagering Scraper
Source: in.gov IGC monthly revenue reports
Format: Excel (.xlsx), one per month, predictable URL pattern
Launch: September 2019
Tax: 9.5% on Adjusted Gross Revenue
Note: Sheet 8 "SW Details" has operator+brand level data in 3-zone layout
      Sheet 7 "SW Tax Summary" has casino-level tax + statewide sport breakdown
"""

import sys
import re
import calendar
from pathlib import Path
from datetime import date, datetime

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger, fetch_with_retry, download_file

REPORT_URL_TEMPLATE = "https://www.in.gov/igc/files/reports/{year}/{year}-{month:02d}-Revenue.xlsx"

# Indiana sports betting launch: September 2019
IN_START_YEAR = 2019
IN_START_MONTH = 9

# Sheet 8 three-zone column layout (0-indexed)
ZONES = [
    {"name": "Northern",  "label_col": 0,  "handle_col": 1,  "ggr_col": 3},
    {"name": "Southern",  "label_col": 5,  "handle_col": 6,  "ggr_col": 8},
    {"name": "Racino",    "label_col": 10, "handle_col": 11, "ggr_col": 13},
]


class INScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("IN")

    def discover_periods(self) -> list[dict]:
        """Generate URLs for all months from launch to current."""
        periods = []
        today = date.today()

        year = IN_START_YEAR
        month = IN_START_MONTH

        while True:
            last_day = calendar.monthrange(year, month)[1]
            period_end = date(year, month, last_day)

            if period_end > today:
                break

            url = REPORT_URL_TEMPLATE.format(year=year, month=month)
            periods.append({
                "download_url": url,
                "period_end": period_end,
                "period_type": "monthly",
                "year": year,
                "month": month,
            })

            month += 1
            if month > 12:
                month = 1
                year += 1

        return periods

    def download_report(self, period_info: dict) -> Path:
        """Download the monthly revenue XLSX."""
        url = period_info["download_url"]
        year = period_info["year"]
        month = period_info["month"]
        filename = f"IN_{year}-{month:02d}-Revenue.xlsx"
        save_path = self.raw_dir / filename

        if not self._should_redownload(save_path):
            return save_path

        try:
            download_file(url, save_path)
            self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
        except Exception as e:
            self.logger.warning(f"  Download failed for {filename}: {e}")
            raise

        return save_path

    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        """Parse an IN monthly Excel file (Sheet 8 for detail, Sheet 7 for tax)."""
        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return pd.DataFrame()

        period_end = period_info["period_end"]

        # Find the SW Details sheet (Sheet 8 or "8 SW Details")
        detail_sheet = self._find_sheet(xls, ["8 SW Details", "Sheet8", "Sheet8 "])
        if detail_sheet is None:
            self.logger.warning(f"  No SW Details sheet in {file_path.name}")
            return pd.DataFrame()

        # Find the SW Tax Summary sheet (Sheet 7 or "7 SW Tax Summary")
        tax_sheet = self._find_sheet(xls, ["7 SW Tax Summary", "Sheet7"])

        # Parse detail sheet for operator-level handle + gross receipts
        df_detail = pd.read_excel(file_path, sheet_name=detail_sheet, header=None)
        rows = self._parse_detail_sheet(df_detail, period_end)

        # Parse tax sheet for casino-level tax + sport breakdown
        tax_by_casino = {}
        if tax_sheet:
            df_tax = pd.read_excel(file_path, sheet_name=tax_sheet, header=None)
            tax_by_casino = self._parse_tax_sheet(df_tax)

        # Assign tax to casino-level aggregations
        for row in rows:
            casino = row.get("_casino", "")
            if casino in tax_by_casino:
                # Distribute tax proportionally by gross_revenue
                pass  # Tax only at casino level, not brand level

        source_url = period_info.get('download_url', period_info.get('url', None))

        all_rows = []
        for row in rows:
            record = {
                "period_end": period_end,
                "period_type": "monthly",
                "operator_raw": row["operator_raw"],
                "channel": row["channel"],
                "handle": row.get("handle"),
                "gross_revenue": row.get("gross_revenue"),
            }

            # Derive standard_ggr and payouts
            if row.get("gross_revenue") is not None:
                record["standard_ggr"] = row["gross_revenue"]
            if row.get("handle") is not None and row.get("gross_revenue") is not None:
                record["payouts"] = row["handle"] - row["gross_revenue"]

            # Add per-row net_revenue
            if row.get("net_revenue") is not None:
                record["net_revenue"] = row["net_revenue"]

            # Source provenance fields
            record["source_file"] = file_path.name
            record["source_sheet"] = detail_sheet
            record["source_row"] = row.get("source_row")
            record["source_url"] = source_url
            record["source_raw_line"] = row.get("source_raw_line")
            record["source_context"] = row.get("source_context")

            all_rows.append(record)

        # Parse sport breakdown from tax sheet (rows 24-30: "State Wide Handle by Sport")
        if tax_sheet:
            sport_rows = self._parse_sport_breakdown(df_tax, period_end)
            for sr in sport_rows:
                sr["source_file"] = file_path.name
                sr["source_sheet"] = tax_sheet
                sr["source_url"] = source_url
            all_rows.extend(sport_rows)

        if not all_rows:
            return pd.DataFrame()

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        return result

    def _find_sheet(self, xls: pd.ExcelFile, candidates: list[str]) -> str | None:
        """Find first matching sheet name from candidates."""
        sheet_names_lower = {s.strip().lower(): s for s in xls.sheet_names}
        for cand in candidates:
            cand_lower = cand.strip().lower()
            if cand_lower in sheet_names_lower:
                return sheet_names_lower[cand_lower]
        return None

    def _parse_detail_sheet(self, df: pd.DataFrame, period_end: date) -> list[dict]:
        """Parse Sheet 8 (SW Details) three-zone layout."""
        rows = []

        for zone in ZONES:
            label_col = zone["label_col"]
            handle_col = zone["handle_col"]
            ggr_col = zone["ggr_col"]

            # Ensure columns exist
            if ggr_col >= df.shape[1]:
                continue

            current_casino = None
            casino_rows = []  # Accumulate rows for one casino block

            i = 0
            while i < len(df):
                label_val = df.iloc[i, label_col]

                if pd.isna(label_val) or str(label_val).strip() == "":
                    i += 1
                    continue

                label = str(label_val).strip()
                label_lower = label.lower()

                # Skip header rows
                if label_lower in ("indiana gaming commission", "detail of sports wagering",
                                   "northern licensees", "southern licenees", "racino licenees",
                                   "southern licensees", "racino licensees"):
                    i += 1
                    continue

                # Skip footnotes and notes
                if (label_lower.startswith("note") or label.startswith("*") or
                    label.startswith("**") or "corrected the amounts" in label_lower or
                    "finalized" in label_lower):
                    i += 1
                    continue

                # Normalize asterisk-annotated labels
                label = label.rstrip("*").strip()
                label_lower = label.lower()

                # Check if this is a casino header (next cell says "Handle")
                handle_header = df.iloc[i, handle_col] if handle_col < df.shape[1] else None
                is_casino_header = (
                    pd.notna(handle_header) and
                    str(handle_header).strip().lower() == "handle"
                )

                if is_casino_header:
                    # Save previous casino block
                    if current_casino and casino_rows:
                        rows.extend(self._finalize_casino_block(current_casino, casino_rows))
                    current_casino = label
                    current_casino_row = i
                    casino_rows = []
                    i += 1
                    continue

                if current_casino is None:
                    i += 1
                    continue

                # Parse data rows within a casino block
                # Capture raw cell values from this row in the zone
                _raw_cells = [str(v) for c in [label_col, handle_col, ggr_col]
                              if c < df.shape[1] and pd.notna(v := df.iloc[i, c]) and str(v).strip()]
                _source_raw_line = ' | '.join(_raw_cells)

                # Build source context for dashboard visual
                _context_json = self.build_source_context(df, current_casino_row, i)

                if label_lower == "taxable agr":
                    taxable_val = self._parse_money(df.iloc[i, ggr_col])
                    casino_rows.append({
                        "type": "taxable_agr",
                        "value": taxable_val,
                        "source_row": i + 1,  # 1-indexed Excel row
                        "source_raw_line": _source_raw_line,
                        "source_context": _context_json,
                    })
                elif label_lower.startswith("adjustment"):
                    adj_val = self._parse_money(df.iloc[i, ggr_col])
                    casino_rows.append({
                        "type": "adjustments",
                        "value": adj_val,
                        "source_row": i + 1,
                        "source_raw_line": _source_raw_line,
                        "source_context": _context_json,
                    })
                elif label_lower.startswith("retail"):
                    handle = self._parse_money(df.iloc[i, handle_col])
                    ggr = self._parse_money(df.iloc[i, ggr_col])
                    casino_rows.append({
                        "type": "retail",
                        "label": "Retail",
                        "handle": handle,
                        "gross_revenue": ggr,
                        "source_row": i + 1,
                        "source_raw_line": _source_raw_line,
                        "source_context": _context_json,
                    })
                elif label_lower.startswith("wc "):
                    # Winner's Circle (off-track) locations — treat as retail
                    handle = self._parse_money(df.iloc[i, handle_col])
                    ggr = self._parse_money(df.iloc[i, ggr_col])
                    casino_rows.append({
                        "type": "wc",
                        "label": label,
                        "handle": handle,
                        "gross_revenue": ggr,
                        "source_row": i + 1,
                        "source_raw_line": _source_raw_line,
                        "source_context": _context_json,
                    })
                else:
                    # Online brand row (e.g., "AS - Sportsbook.DraftKings.com")
                    handle = self._parse_money(df.iloc[i, handle_col])
                    ggr = self._parse_money(df.iloc[i, ggr_col])
                    casino_rows.append({
                        "type": "online",
                        "label": label,
                        "handle": handle,
                        "gross_revenue": ggr,
                        "source_row": i + 1,
                        "source_raw_line": _source_raw_line,
                        "source_context": _context_json,
                    })

                i += 1

            # Don't forget the last casino block in the zone
            if current_casino and casino_rows:
                rows.extend(self._finalize_casino_block(current_casino, casino_rows))

        return rows

    def _finalize_casino_block(self, casino_name: str, block_rows: list[dict]) -> list[dict]:
        """Convert a casino block into output rows."""
        results = []
        taxable_agr = None
        adjustments = 0.0

        # Find taxable AGR and adjustments
        for r in block_rows:
            if r["type"] == "taxable_agr":
                taxable_agr = r["value"]
            elif r["type"] == "adjustments":
                if r.get("value") is not None:
                    adjustments += r["value"]

        # Aggregate retail + WC rows
        retail_handle = 0.0
        retail_ggr = 0.0
        has_retail = False

        for r in block_rows:
            if r["type"] in ("retail", "wc"):
                has_retail = True
                if r.get("handle") is not None:
                    retail_handle += r["handle"]
                if r.get("gross_revenue") is not None:
                    retail_ggr += r["gross_revenue"]

        if has_retail:
            # Use the source_row from the first retail/wc row
            first_retail_row = next(
                (r.get("source_row") for r in block_rows if r["type"] in ("retail", "wc")),
                None
            )
            # Use source_context from the first retail/wc row
            first_retail_context = next(
                (r.get("source_context") for r in block_rows if r["type"] in ("retail", "wc")),
                None
            )
            # Combine raw lines from all retail/wc rows
            retail_raw_lines = [r.get("source_raw_line", "") for r in block_rows
                                if r["type"] in ("retail", "wc") and r.get("source_raw_line")]
            results.append({
                "operator_raw": casino_name,
                "channel": "retail",
                "handle": retail_handle if retail_handle else None,
                "gross_revenue": retail_ggr if retail_ggr else None,
                "_casino": casino_name,
                "source_row": first_retail_row,
                "source_raw_line": ' || '.join(retail_raw_lines) if retail_raw_lines else None,
                "source_context": first_retail_context,
            })

        # Online brand rows
        for r in block_rows:
            if r["type"] == "online":
                brand_name = self._extract_brand_name(r["label"])
                results.append({
                    "operator_raw": brand_name,
                    "channel": "online",
                    "handle": r.get("handle"),
                    "gross_revenue": r.get("gross_revenue"),
                    "_casino": casino_name,
                    "source_row": r.get("source_row"),
                    "source_raw_line": r.get("source_raw_line"),
                    "source_context": r.get("source_context"),
                })

        # Distribute adjustments proportionally to compute per-row net_revenue
        # net_revenue = gross_revenue + (gross_revenue / total_gross) * adjustments
        if results and taxable_agr is not None:
            total_gross = sum(
                r.get("gross_revenue", 0) or 0 for r in results
            )
            for r in results:
                gr = r.get("gross_revenue")
                if gr is not None and total_gross != 0:
                    share = gr / total_gross
                    r["net_revenue"] = gr + share * adjustments
                elif gr is not None:
                    # All gross_revenue is zero; distribute adjustments evenly
                    r["net_revenue"] = gr + adjustments / len(results)
                else:
                    r["net_revenue"] = None

        return results

    def _extract_brand_name(self, raw_label: str) -> str:
        """Extract operator brand from 'XX - domain.com' format."""
        # e.g., "AS - Sportsbook.DraftKings.com" -> "DraftKings"
        # e.g., "BC - in.sportsbook.FanDuel.com" -> "FanDuel"
        # e.g., "HP - WilliamHill.com" -> "William Hill"
        if " - " in raw_label:
            domain_part = raw_label.split(" - ", 1)[1].strip()
            # Try to extract brand from domain
            domain_lower = domain_part.lower()
            if "draftkings" in domain_lower or "draftkings" in domain_lower:
                return "DraftKings"
            elif "fanduel" in domain_lower:
                return "FanDuel"
            elif "betmgm" in domain_lower:
                return "BetMGM"
            elif "caesars" in domain_lower:
                return "Caesars"
            elif "williamhill" in domain_lower:
                return "William Hill"
            elif "hardrock" in domain_lower:
                return "Hard Rock Bet"
            elif "ballybet" in domain_lower:
                return "Bally Bet"
            elif "betrivers" in domain_lower:
                return "BetRivers"
            elif "bet365" in domain_lower:
                return "bet365"
            elif "fanatics" in domain_lower:
                return "Fanatics"
            elif "espnbet" in domain_lower or "thescore" in domain_lower:
                return "ESPN BET"
            elif "pointsbet" in domain_lower:
                return "PointsBet"
            elif "barstool" in domain_lower:
                return "Barstool"
            elif "unibet" in domain_lower:
                return "Unibet"
            elif "betway" in domain_lower:
                return "Betway"
            elif "maximbet" in domain_lower:
                return "MaximBet"
            elif "wynnbet" in domain_lower:
                return "WynnBET"
            elif "twinspires" in domain_lower:
                return "TwinSpires"
            elif "getsbk" in domain_lower:
                return "SuperBook"
            elif "smarkets" in domain_lower:
                return "Smarkets"
            elif "betamerica" in domain_lower:
                return "BetAmerica"
            # Fallback: use the domain
            return domain_part
        return raw_label

    def _parse_sport_breakdown(self, df: pd.DataFrame, period_end) -> list[dict]:
        """Parse 'State Wide Handle by Sport' from Sheet 7 (SW Tax Summary).

        Located after the casino rows, typically at row 24+:
          State Wide Handle by Sport    Month         YTD
          Football                      $19,342,260   $800,589,086
          Basketball                    $171,637,327  $802,793,586
          Baseball                      $1,436,038    $240,520,274
          Parlay                        $137,500,422  $1,330,349,613
          Other                         $100,736,731  $744,211,693
          TOTAL                         $430,652,780  $3,918,464,252
        """
        rows = []
        in_sport_section = False

        for i in range(len(df)):
            val = df.iloc[i, 0]
            if pd.isna(val):
                continue
            label = str(val).strip()

            if "handle by sport" in label.lower():
                in_sport_section = True
                continue

            if not in_sport_section:
                continue

            if label.lower() in ("total", "note:") or label.lower().startswith("note:"):
                break

            # Parse month handle (column 2)
            handle = None
            if df.shape[1] > 2 and pd.notna(df.iloc[i, 2]):
                try:
                    handle = float(df.iloc[i, 2])
                except (ValueError, TypeError):
                    pass

            if handle is not None and label:
                # Capture raw cell values from this row
                raw_cells = [str(v) for c in range(min(5, df.shape[1]))
                             if pd.notna(v := df.iloc[i, c]) and str(v).strip()]
                rows.append({
                    "period_end": period_end,
                    "period_type": "monthly",
                    "operator_raw": "ALL",
                    "channel": "combined",
                    "sport_category": label,
                    "handle": handle,
                    "source_row": i + 1,  # 1-indexed Excel row
                    "source_raw_line": ' | '.join(raw_cells),
                })

        return rows

    def _parse_tax_sheet(self, df: pd.DataFrame) -> dict:
        """Parse Sheet 7 for per-casino tax amounts. Returns {casino_name: tax_amount}."""
        tax_map = {}

        # Find the header row with "Handle", "Taxable AGR", "Tax"
        header_row = None
        for i in range(min(10, len(df))):
            val = df.iloc[i, 0] if df.shape[1] > 0 else None
            if pd.notna(val) and "sports wagering agr" in str(val).strip().lower():
                header_row = i
                break

        if header_row is None:
            return tax_map

        # Determine tax column position (new format: col 3, old format: col 7)
        # Check header row for "Tax" position
        tax_col = None
        for c in range(df.shape[1]):
            val = df.iloc[header_row, c]
            if pd.notna(val) and str(val).strip().lower() == "tax":
                tax_col = c
                break

        if tax_col is None:
            return tax_map

        # Parse casino rows until TOTAL
        for i in range(header_row + 1, len(df)):
            name_val = df.iloc[i, 0]
            if pd.isna(name_val):
                continue
            name = str(name_val).strip()
            if not name or name.upper() == "TOTAL":
                break

            tax_val = self._parse_money(df.iloc[i, tax_col])
            if tax_val is not None:
                tax_map[name] = tax_val

        return tax_map

    def _parse_money(self, value) -> float | None:
        """Parse a money value from Excel cell."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            v = float(value)
            if v == 0:
                return 0.0
            return v
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
    scraper = INScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"IN SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Period types: {df['period_type'].value_counts().to_dict()}")
        print(f"Operators: {df['operator_standard'].nunique()}")
        print(f"Channels: {df['channel'].value_counts().to_dict()}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        print(f"\nPer-operator row counts:")
        for op in sorted(df['operator_standard'].unique()):
            count = len(df[df['operator_standard'] == op])
            print(f"  {op}: {count}")

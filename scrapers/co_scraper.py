"""
Colorado Sports Betting Scraper
Source: SBG Colorado monthly PDF reports
Format: Two PDF types per month:
  - "Sports Betting Proceeds Report" (Monthly Summary): Handle, GGR, NSBP, sport breakdown
    Image-based PDFs requiring OCR (PyMuPDF + Tesseract)
    Columns: Retail, Online, Total (month name)
  - "HB 1292" report: NSBP and tax by city (Black Hawk, Central City, Cripple Creek)
    Text-based PDFs parseable with pdfplumber
Launch: May 2020
Tax: 10% on NSBP (Net Sports Betting Proceeds = GGR minus promo deductions)
Note: No operator detail. Monthly Summary has statewide aggregate handle/GGR per channel.
      HB 1292 has city-level NSBP/tax with retail/online split.
"""

import sys
import re
import io
import calendar
from pathlib import Path
from datetime import date, datetime
from urllib.parse import unquote

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.base_scraper import BaseStateScraper
from scrapers.scraper_utils import setup_logger

# Try OCR dependencies
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    import pytesseract
    from PIL import Image
except ImportError:
    pytesseract = None

CO_REPORTS_URL = "https://sbg.colorado.gov/sports-betting-monthly-reports"
CO_BASE_URL = "https://sbg.colorado.gov"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

MONTH_PATTERN = "|".join(MONTH_MAP.keys())

SPORT_NAME_MAP = {
    "football - pro american": "NFL", "football pro american": "NFL",
    "pro football": "NFL", "football": "NFL",
    "basketball": "NBA",
    "ncaa football": "NCAAF", "college football": "NCAAF",
    "ncaa basketball": "NCAAB", "college basketball": "NCAAB",
    "baseball": "MLB", "ncaa baseball": "College Baseball",
    "hockey - ice": "NHL", "hockey ice": "NHL", "ice hockey": "NHL", "hockey": "NHL",
    "soccer": "Soccer", "tennis": "Tennis", "mma": "MMA", "boxing": "Boxing",
    "golf": "Golf", "table tennis": "Table Tennis", "motorsports": "Motorsports",
    "volleyball": "Volleyball", "darts": "Darts", "rugby": "Rugby",
    "cricket": "Cricket", "cycling": "Cycling", "lacrosse": "Lacrosse",
    "olympics": "Olympics", "esports": "eSports",
    "parlays/combinations": "Parlays", "parlays/combina ons": "Parlays",
    "parlays": "Parlays", "other": "Other",
}


class COScraper(BaseStateScraper):
    def __init__(self):
        super().__init__("CO")
        # Cache: "YYYY-MM" -> {channels: {retail/online/total: {ggr,nsbp,tax,win_pct,handle,payouts}}, sports: {name: {channel: {handle,payouts}}}}
        self._summary_cache = {}

    # ------------------------------------------------------------------
    # discover_periods
    # ------------------------------------------------------------------
    def discover_periods(self) -> list[dict]:
        resp = requests.get(CO_REPORTS_URL, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        }, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        month_data = {}

        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            text_lower = text.lower()

            if ".pdf" not in href.lower():
                continue
            if "year" in text_lower or "fy " in text_lower:
                continue

            period_end = self._extract_date(text_lower, href)
            if not period_end:
                continue

            key = f"{period_end.year}-{period_end.month:02d}"
            if key not in month_data:
                month_data[key] = {
                    "period_end": period_end,
                    "period_type": "monthly",
                    "summary_url": None,
                    "hb1292_url": None,
                }

            full_url = href if href.startswith("http") else CO_BASE_URL + href

            if "1292" in text_lower or "hb" in text_lower:
                month_data[key]["hb1292_url"] = full_url
            elif "sports betting proceeds report" in text_lower:
                month_data[key]["summary_url"] = full_url
            elif "proceeds report" in text_lower and "1292" not in text_lower:
                month_data[key]["summary_url"] = full_url

        periods = sorted(month_data.values(), key=lambda p: p["period_end"])

        summary_count = sum(1 for p in periods if p.get("summary_url"))
        hb1292_count = sum(1 for p in periods if p.get("hb1292_url"))
        self.logger.info(
            f"  Found {len(periods)} months: "
            f"{summary_count} Monthly Summary, {hb1292_count} HB 1292"
        )
        return periods

    def _extract_date(self, text: str, url: str) -> date | None:
        combined = text + " " + unquote(url).lower()
        for month_name, month_num in MONTH_MAP.items():
            if month_name in combined:
                year_match = re.search(rf'{month_name}\s+(\d{{4}})', combined)
                if year_match:
                    year = int(year_match.group(1))
                    last_day = calendar.monthrange(year, month_num)[1]
                    return date(year, month_num, last_day)
        return None

    # ------------------------------------------------------------------
    # download_report
    # ------------------------------------------------------------------
    def download_report(self, period_info: dict) -> Path:
        period_end = period_info["period_end"]
        prefix = f"CO_{period_end.year}_{period_end.month:02d}"

        hb1292_path = None
        if period_info.get("hb1292_url"):
            hb1292_path = self._download_pdf(
                period_info["hb1292_url"], f"{prefix}_hb1292.pdf"
            )

        summary_path = None
        if period_info.get("summary_url"):
            summary_path = self._download_pdf(
                period_info["summary_url"], f"{prefix}_summary.pdf"
            )

        if summary_path:
            self._parse_summary_pdf(summary_path, period_end)

        return hb1292_path or summary_path or Path("/dev/null")

    def _download_pdf(self, url: str, filename: str) -> Path | None:
        save_path = self.raw_dir / filename
        if not self._should_redownload(save_path):
            return save_path

        try:
            resp = requests.get(url, headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "gzip, deflate",
                "Referer": CO_REPORTS_URL,
            }, timeout=60)
            if resp.status_code != 200:
                self.logger.warning(f"  {filename}: HTTP {resp.status_code}")
                return None
            if len(resp.content) < 1000:
                return None
            with open(save_path, "wb") as f:
                f.write(resp.content)
            self.logger.info(f"  Downloaded: {filename} ({save_path.stat().st_size:,} bytes)")
            return save_path
        except Exception as e:
            self.logger.warning(f"  Failed to download {filename}: {e}")
            return None

    # ------------------------------------------------------------------
    # parse_report — creates separate retail and online rows
    # ------------------------------------------------------------------
    def parse_report(self, file_path: Path, period_info: dict) -> pd.DataFrame:
        period_end = period_info["period_end"]
        key = f"{period_end.year}-{period_end.month:02d}"
        all_rows = []

        # Capture page 1 as a PNG screenshot for provenance
        # Prefer the summary PDF (main financial data); fall back to hb1292
        summary_path = self.raw_dir / f"CO_{period_end.year}_{period_end.month:02d}_summary.pdf"
        capture_path = summary_path if summary_path.exists() else file_path
        screenshot_path = None
        if capture_path.exists() and str(capture_path) != "/dev/null":
            screenshot_path = self.capture_pdf_page(capture_path, 1, period_info)

        # Source provenance
        source_file = file_path.name
        source_url = period_info.get('download_url', period_info.get('url', None))
        # CO has two PDFs per month; prefer hb1292_url as the primary download_url
        if source_url is None:
            source_url = period_info.get('hb1292_url', period_info.get('summary_url', None))

        # Get HB 1292 statewide per-channel NSBP/tax
        hb_data = self._get_hb1292_totals(period_end)

        # Get per-channel data from cached summary
        summary = self._summary_cache.get(key, {})
        channels = summary.get("channels", {})

        # If no summary cached, try OCR on existing summary PDF
        if not summary:
            summary_path = self.raw_dir / f"CO_{period_end.year}_{period_end.month:02d}_summary.pdf"
            if summary_path.exists():
                self._parse_summary_pdf(summary_path, period_end)
                summary = self._summary_cache.get(key, {})
                channels = summary.get("channels", {})

        is_combined = summary.get("combined_format", False)

        if is_combined:
            # Early months where retail & online were combined for confidentiality
            ch = channels.get("total", channels.get("combined", {}))
            hb_combined = {
                "nsbp": hb_data.get("retail", {}).get("nsbp", 0) + hb_data.get("online", {}).get("nsbp", 0),
                "tax": hb_data.get("retail", {}).get("tax", 0) + hb_data.get("online", {}).get("tax", 0),
            }
            row = self._make_row(period_end, "combined", ch, hb_combined)
            if row:
                all_rows.append(row)
        else:
            # Create separate retail and online rows
            for ch_name in ["retail", "online"]:
                ch = channels.get(ch_name, {})
                hb = hb_data.get(ch_name, {})
                row = self._make_row(period_end, ch_name, ch, hb)
                if row:
                    all_rows.append(row)

        # Sport breakdown rows — per channel (retail/online)
        sports = summary.get("sports", {})
        for sport_name, sport_channels in sports.items():
            for ch_name, ch_data in sport_channels.items():
                handle = ch_data.get("handle", 0) or 0
                payouts = ch_data.get("payouts", 0) or 0
                if handle and handle > 0:
                    row = {
                        "period_end": period_end,
                        "period_type": "monthly",
                        "operator_raw": "ALL",
                        "channel": ch_name,
                        "sport_category": sport_name,
                        "handle": handle,
                    }
                    if payouts and payouts > 0:
                        row["payouts"] = payouts
                    all_rows.append(row)

        if not all_rows:
            return pd.DataFrame()

        # Add provenance fields to every row
        for row in all_rows:
            row["source_file"] = source_file
            row["source_url"] = source_url
            row["source_page"] = 1
            row["source_table_index"] = 0

        result = pd.DataFrame(all_rows)
        result["period_end"] = pd.to_datetime(result["period_end"])
        result["period_start"] = result["period_end"].apply(lambda d: d.replace(day=1))
        if screenshot_path:
            result["source_screenshot"] = screenshot_path
        return result

    def _make_row(self, period_end, channel, ch_data, hb_data):
        """Build a single row dict from channel data and HB1292 data."""
        row = {
            "period_end": period_end,
            "period_type": "monthly",
            "operator_raw": "ALL",
            "channel": channel,
            "source_raw_line": ch_data.get("source_raw_line"),
        }

        handle = ch_data.get("handle", 0) or 0
        ggr = ch_data.get("ggr", 0) or 0
        payouts_val = ch_data.get("payouts", 0) or 0
        # Prefer summary NSBP/tax (from OCR), fall back to HB1292
        nsbp = ch_data.get("nsbp", 0) or hb_data.get("nsbp", 0) or 0
        tax = ch_data.get("tax", 0) or hb_data.get("tax", 0) or 0

        if handle and abs(handle) > 0:
            row["handle"] = handle
        if ggr and ggr != 0:
            row["gross_revenue"] = ggr
            row["standard_ggr"] = ggr  # CO GGR = Handle - Payouts (no excise)
        if payouts_val and abs(payouts_val) > 0:
            row["payouts"] = payouts_val
        elif handle > 0 and ggr != 0:
            row["payouts"] = handle - ggr
        if nsbp and nsbp != 0:
            row["net_revenue"] = nsbp
        if tax and tax != 0:
            row["tax_paid"] = tax

        # Promo credits = GGR - NSBP
        if ggr > 0 and nsbp > 0:
            promo = ggr - nsbp
            if promo > 0:
                row["promo_credits"] = promo

        # Only return if meaningful data
        if any(row.get(f) for f in ["handle", "gross_revenue", "net_revenue"]):
            return row
        return None

    # ------------------------------------------------------------------
    # HB 1292 parsing (text-based)
    # ------------------------------------------------------------------
    def _get_hb1292_totals(self, period_end: date) -> dict:
        """Parse HB 1292 for statewide per-channel NSBP and tax."""
        hb_path = None
        for name in [
            f"CO_{period_end.year}_{period_end.month:02d}_hb1292.pdf",
            f"CO_{period_end.year}_{period_end.month:02d}.pdf",
        ]:
            p = self.raw_dir / name
            if p.exists() and p.stat().st_size > 1000:
                hb_path = p
                break

        if not hb_path:
            return {}

        city_rows = self._parse_hb1292(hb_path, period_end)
        if not city_rows:
            return {}

        result = {"retail": {"nsbp": 0.0, "tax": 0.0}, "online": {"nsbp": 0.0, "tax": 0.0}}
        for r in city_rows:
            ch = r.get("channel", "")
            if ch in result:
                result[ch]["nsbp"] += r.get("net_revenue", 0) or 0
                result[ch]["tax"] += r.get("tax_paid", 0) or 0

        return result

    def _parse_hb1292(self, file_path: Path, period_end: date) -> list[dict]:
        try:
            pdf = pdfplumber.open(file_path)
        except Exception as e:
            self.logger.error(f"Cannot read {file_path}: {e}")
            return []

        rows = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text:
                continue

            lines = text.splitlines()
            in_monthly = False

            for line in lines:
                line_lower = line.strip().lower()

                if "monthly sports betting proceeds" in line_lower:
                    in_monthly = True
                    continue
                elif "annual sports betting proceeds" in line_lower:
                    in_monthly = False
                    continue

                if not in_monthly:
                    continue

                if not line.strip() or "On-Site" in line or "CURR" in line or "NSBP" in line:
                    continue

                city_match = re.match(
                    r'(Black Hawk|Central City|Cripple Creek|Statewide)\s*\*?\s+(.*)',
                    line.strip()
                )
                if not city_match:
                    continue

                city = city_match.group(1).strip()
                if city.lower() == "statewide":
                    continue

                values_str = city_match.group(2)
                all_vals = []
                for v in re.findall(r'(?:-?\$[\d,]+(?:\.\d+)?|\(\$[\d,]+(?:\.\d+)?\))', values_str):
                    all_vals.append(self._parse_money(v))

                if len(all_vals) >= 6:
                    rows.append({
                        "period_end": period_end,
                        "operator_raw": city,
                        "channel": "retail",
                        "net_revenue": all_vals[0],
                        "tax_paid": all_vals[3],
                        "source_raw_line": line.strip(),
                    })
                    rows.append({
                        "period_end": period_end,
                        "operator_raw": city,
                        "channel": "online",
                        "net_revenue": all_vals[1],
                        "tax_paid": all_vals[4],
                        "source_raw_line": line.strip(),
                    })

        pdf.close()
        return rows

    # ------------------------------------------------------------------
    # Monthly Summary parsing (OCR) — per-channel extraction
    # ------------------------------------------------------------------
    def _parse_summary_pdf(self, file_path: Path, period_end: date):
        key = f"{period_end.year}-{period_end.month:02d}"
        if key in self._summary_cache:
            return

        text = self._extract_text_with_ocr(file_path)
        if not text or len(text) < 50:
            self.logger.warning(f"  No text from summary PDF {file_path.name}")
            return

        text = self._normalize_ocr(text)

        channels, combined_format = self._parse_channels(text)
        sports = self._parse_sports(text)

        total = channels.get("total", {})
        online = channels.get("online", {})
        retail = channels.get("retail", {})
        has_data = (
            total.get("ggr", 0) != 0 or
            total.get("handle", 0) > 0 or
            online.get("ggr", 0) != 0 or
            online.get("handle", 0) > 0 or
            retail.get("ggr", 0) != 0
        )

        if has_data:
            self._summary_cache[key] = {
                "channels": channels,
                "sports": sports,
                "combined_format": combined_format,
            }
            r = channels.get("retail", {})
            o = channels.get("online", {})
            t = channels.get("total", {})
            tag = " [COMBINED]" if combined_format else ""
            self.logger.info(
                f"  Summary {key}: "
                f"R(h=${r.get('handle', 0):,.0f} ggr=${r.get('ggr', 0):,.0f}) "
                f"O(h=${o.get('handle', 0):,.0f} ggr=${o.get('ggr', 0):,.0f}) "
                f"T(h=${t.get('handle', 0):,.0f} ggr=${t.get('ggr', 0):,.0f}), "
                f"{len(sports)} sports{tag}"
            )
        else:
            self.logger.warning(f"  Summary {key}: could not parse data")

    def _extract_text_with_ocr(self, file_path: Path) -> str:
        try:
            with pdfplumber.open(file_path) as pdf:
                parts = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        parts.append(t)
                if parts:
                    text = "\n".join(parts)
                    if len(text) > 100:
                        return text
        except Exception:
            pass

        if fitz and pytesseract:
            try:
                doc = fitz.open(str(file_path))
                parts = []
                for page in doc:
                    mat = fitz.Matrix(300 / 72, 300 / 72)
                    pix = page.get_pixmap(matrix=mat)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    t = pytesseract.image_to_string(img)
                    if t:
                        parts.append(t)
                doc.close()
                return "\n".join(parts)
            except Exception as e:
                self.logger.warning(f"  OCR failed for {file_path.name}: {e}")
        else:
            self.logger.warning(
                "  OCR not available (need PyMuPDF + pytesseract). "
                "Install: pip install PyMuPDF pytesseract"
            )

        return ""

    @staticmethod
    def _normalize_ocr(text: str) -> str:
        """Fix common OCR artifacts in CO summary PDFs."""
        text = text.replace('S$', '$')
        text = re.sub(r'§(\d)', r'$\1', text)
        # "S " followed by dollar-amount-like number → "$"
        text = re.sub(r'\bS\s+(\d{1,3}(?:,\d{3})*\.\d{2})', r'$\1', text)
        # "S " before parenthesized amounts → "$" (OCR reads $ as S)
        text = re.sub(r'\bS\s+\((\d)', r'$(\1', text)
        return text

    # ------------------------------------------------------------------
    # Channel parsing — extract per-channel GGR/NSBP/Tax/Handle
    # ------------------------------------------------------------------
    def _parse_channels(self, text: str) -> tuple[dict, bool]:
        """Parse per-channel data. Returns (channels_dict, is_combined_format)."""
        lines = text.split("\n")
        ch = {name: {"ggr": 0.0, "nsbp": 0.0, "tax": 0.0, "win_pct": 0.0, "handle": 0.0, "payouts": 0.0}
              for name in ["retail", "online", "total"]}

        # Find key boundaries
        wagers_idx = len(lines)
        payments_idx = len(lines)
        for i, line in enumerate(lines):
            stripped = line.strip()
            if re.search(r"^WAGERS\b", stripped, re.I) and wagers_idx == len(lines):
                wagers_idx = i
            if re.search(r"PAYMENTS\s+TO\s+PLAYERS", stripped, re.I) and payments_idx == len(lines):
                payments_idx = i

        # Find month header (can be before or after WAGERS)
        month_idx = None
        for i, line in enumerate(lines):
            if re.search(rf"^(?:{MONTH_PATTERN})\s+20\d{{2}}\s*$", line.strip(), re.I):
                month_idx = i
                break

        # Detect tabular format (pdfplumber structured text: "Total GGR* $R $O $T")
        # Require >= 2 metric lines with >= 2 amounts each (avoids OCR false positives)
        tabular_count = 0
        for line in lines:
            if re.search(r'Total\s+(GGR|NSBP|Taxes)', line, re.I) and len(self._find_amounts(line)) >= 2:
                tabular_count += 1
        tabular = tabular_count >= 2

        if tabular:
            full_text_lower = "\n".join(lines).lower()
            combined_format = "retail & online" in full_text_lower or "retail and online" in full_text_lower
            self._parse_tabular(lines, ch, combined_format)

            # If tabular didn't get all values (scattered OCR), post-process
            if not combined_format:
                handles_incomplete = (ch["online"]["handle"] == 0 and ch["total"]["handle"] == 0)
                tax_missing = (ch["retail"]["tax"] == 0 and ch["online"]["tax"] == 0
                               and ch["total"]["tax"] == 0)
                winpct_missing = (ch["retail"]["win_pct"] == 0 and ch["online"]["win_pct"] == 0)
                if handles_incomplete or tax_missing or winpct_missing:
                    self._parse_tabular_post(lines, ch)

            self._derive_channel_values(ch, combined_format)
            return ch, combined_format

        # Detect OCR format from pre-WAGERS lines
        pre_wagers_text = "\n".join(lines[:wagers_idx]).lower()
        combined_format = "retail & online" in pre_wagers_text or "retail and online" in pre_wagers_text
        paired_format = bool(re.search(r"retail\s+online", pre_wagers_text, re.I))

        # --- Parse Retail/Online from pre-WAGERS section ---
        if combined_format:
            self._parse_combined_section(lines[:wagers_idx], ch)
        elif paired_format:
            self._parse_paired_section(lines[:wagers_idx], ch)
        else:
            self._parse_sequential_section(lines[:wagers_idx], ch)

        # --- Parse Total from month header section (wherever it appears) ---
        if month_idx is not None:
            self._parse_total_section(lines, ch, month_idx, paired_format)

        # --- Parse handle from WAGERS section ---
        self._parse_wagers_handle(lines, ch, wagers_idx, payments_idx, month_idx)

        # --- Derive missing values ---
        self._derive_channel_values(ch, combined_format)

        return ch, combined_format

    def _parse_combined_section(self, lines: list[str], ch: dict):
        """Parse early format where Retail & Online are combined."""
        # Find "Retail & Online" header
        for i, line in enumerate(lines):
            if re.search(r"retail\s*&\s*online", line, re.I):
                amounts = []
                win_pct = None
                for j in range(i + 1, min(i + 10, len(lines))):
                    text = lines[j].strip()
                    if not text:
                        continue
                    pcts = re.findall(r"(-?\d+\.\d+)%", text)
                    if pcts and win_pct is None:
                        win_pct = float(pcts[0])
                        break
                    line_amounts = self._find_amounts(text)
                    if line_amounts:
                        amounts.extend(line_amounts)

                if len(amounts) >= 3:
                    ch["total"]["ggr"] = amounts[0]
                    ch["total"]["nsbp"] = amounts[1]
                    ch["total"]["tax"] = amounts[2]
                elif len(amounts) >= 1:
                    ch["total"]["ggr"] = amounts[0]
                if win_pct is not None:
                    ch["total"]["win_pct"] = win_pct
                break

    def _parse_paired_section(self, lines: list[str], ch: dict):
        """Parse modern format: 'Retail Online' header with paired values.

        In some OCR layouts, paired R/O values are separated by Total column values.
        We skip empty lines and 1-amount lines (Total values) to find the 2-amount pairs.
        """
        for i, line in enumerate(lines):
            if re.search(r"Retail\s+Online", line, re.I):
                metrics = ["ggr", "nsbp", "tax"]
                metric_idx = 0
                for idx in range(i + 1, min(i + 30, len(lines))):
                    if metric_idx >= len(metrics):
                        break
                    text = lines[idx].strip()
                    if not text:
                        continue
                    # Skip percentage-only lines
                    if re.search(r'\d+\.\d+%', text) and '$' not in text:
                        continue
                    amounts = self._find_amounts(text)
                    # Only use 2+ amount lines (R/O pairs); skip 1-amount (Total values)
                    if len(amounts) >= 2:
                        ch["retail"][metrics[metric_idx]] = amounts[0]
                        ch["online"][metrics[metric_idx]] = amounts[1]
                        metric_idx += 1
                break

        # Win percentages: look for line with 2+ percentages
        for line in lines:
            pcts = re.findall(r"(-?\d+\.\d+)%", line)
            if len(pcts) >= 3:
                ch["retail"]["win_pct"] = float(pcts[0])
                ch["online"]["win_pct"] = float(pcts[1])
                ch["total"]["win_pct"] = float(pcts[2])
                break
            elif len(pcts) == 2:
                ch["retail"]["win_pct"] = float(pcts[0])
                ch["online"]["win_pct"] = float(pcts[1])
                break

    def _parse_sequential_section(self, lines: list[str], ch: dict):
        """Parse early/mid format: separate Retail, Online sections before WAGERS."""
        sections = []  # (line_index, section_type)

        for i, line in enumerate(lines):
            stripped = line.strip()
            if re.match(r'^Retail\*?\s*$', stripped, re.I):
                # Skip Retail headers in sport wagers section (preceded by "Top 10 Sports")
                in_sports = any(
                    re.search(r'Top\s+\d+\s+Sports|Total\s+Wagers', lines[k], re.I)
                    for k in range(max(0, i - 5), i)
                )
                if not in_sports:
                    sections = [(idx, st) for idx, st in sections if st != 'retail']
                    sections.append((i, 'retail'))
            elif re.match(r'^Online\s*$', stripped, re.I):
                sections.append((i, 'online'))

        # Fallback: if no "Online" header found, look for "Colorado Sports Betting"
        if not any(st == 'online' for _, st in sections):
            for i, line in enumerate(lines):
                if re.search(r'colorado\s+sports\s+betting', line, re.I):
                    sections.append((i, 'online'))
                    break

        sections.sort(key=lambda x: x[0])

        for idx, (start, section_type) in enumerate(sections):
            end = len(lines)
            if idx + 1 < len(sections):
                end = sections[idx + 1][0]

            amounts = []
            win_pct = None
            for j in range(start + 1, min(end, start + 12)):
                if j >= len(lines):
                    break
                text = lines[j].strip()
                if not text:
                    continue
                if any(kw in text.lower() for kw in [
                    'statewide', 'summary', 'total ggr', 'total nsbp', 'total taxes',
                    'total win', 'colorado', 'proceeds', 'top 10', 'sports by',
                ]):
                    continue

                pcts = re.findall(r"(-?\d+\.\d+)%", text)
                if pcts and win_pct is None:
                    win_pct = float(pcts[0])
                    break

                line_amounts = self._find_amounts(text)
                if line_amounts:
                    amounts.extend(line_amounts)

            if len(amounts) >= 3:
                ch[section_type]["ggr"] = amounts[0]
                ch[section_type]["nsbp"] = amounts[1]
                ch[section_type]["tax"] = amounts[2]
            elif len(amounts) >= 2:
                ch[section_type]["ggr"] = amounts[0]
                ch[section_type]["nsbp"] = amounts[1]
            elif len(amounts) >= 1:
                ch[section_type]["ggr"] = amounts[0]

            if win_pct is not None:
                ch[section_type]["win_pct"] = win_pct

    def _parse_total_section(self, lines: list[str], ch: dict, month_idx: int, paired_format: bool):
        """Parse Total values from the section after the month header.

        In paired format, 2-amount lines are R/O pairs — skip them and only
        collect 1-amount lines which are the Total column.
        """
        amounts = []
        win_pct = None

        for j in range(month_idx + 1, min(month_idx + 20, len(lines))):
            text = lines[j].strip()
            if not text:
                continue
            # Stop at section boundaries (but not "Retail"/"Online" within Statewide Summary header)
            if re.search(r'^(WAGERS|PAYMENTS|Top\s+\d+\s+Sports)', text, re.I):
                break

            pcts = re.findall(r"(-?\d+\.\d+)%", text)
            if pcts and win_pct is None:
                if paired_format and len(pcts) >= 3:
                    ch["retail"]["win_pct"] = float(pcts[0])
                    ch["online"]["win_pct"] = float(pcts[1])
                    win_pct = float(pcts[2])
                elif len(pcts) == 1:
                    win_pct = float(pcts[0])
                else:
                    win_pct = float(pcts[-1])
                break

            line_amounts = self._find_amounts(text)

            # In paired format, skip 2+ amount lines (those are R/O pairs)
            if paired_format and len(line_amounts) >= 2:
                continue

            if line_amounts:
                amounts.extend(line_amounts)

        if len(amounts) >= 3:
            ch["total"]["ggr"] = amounts[0]
            ch["total"]["nsbp"] = amounts[1]
            ch["total"]["tax"] = amounts[2]
        elif len(amounts) >= 2:
            ch["total"]["ggr"] = amounts[0]
            ch["total"]["nsbp"] = amounts[1]
        elif len(amounts) >= 1:
            ch["total"]["ggr"] = amounts[0]

        if win_pct is not None:
            ch["total"]["win_pct"] = win_pct

    def _parse_tabular(self, lines: list[str], ch: dict, is_combined: bool = False):
        """Parse tabular format where pdfplumber extracts 'Total GGR* $R $O $T' lines.

        For combined format (e.g. Jun 2020), columns are [Combined, Total] instead of [R, O, T].
        """
        metrics_map = [
            (r'Total\s+GGR', 'ggr'),
            (r'Total\s+NSBP', 'nsbp'),
            (r'Total\s+Taxes', 'tax'),
        ]

        for line in lines:
            for pattern, metric in metrics_map:
                if re.search(pattern, line, re.I):
                    amounts = self._find_amounts(line)
                    if is_combined:
                        # [Combined, Total] — store last as total
                        if amounts:
                            ch["total"][metric] = amounts[-1]
                    elif len(amounts) >= 3:
                        ch["retail"][metric] = amounts[0]
                        ch["online"][metric] = amounts[1]
                        ch["total"][metric] = amounts[2]
                    elif len(amounts) == 2:
                        ch["retail"][metric] = amounts[0]
                        ch["online"][metric] = amounts[1]
                    break

            # Win percentage — only if actual % format (not "$0.10" artifact)
            if re.search(r'Total\s+Win\s+Percentage', line, re.I):
                pcts = re.findall(r'(-?\d+\.\d+)%', line)
                if is_combined:
                    if pcts:
                        ch["total"]["win_pct"] = float(pcts[-1])
                elif len(pcts) >= 3:
                    ch["retail"]["win_pct"] = float(pcts[0])
                    ch["online"]["win_pct"] = float(pcts[1])
                    ch["total"]["win_pct"] = float(pcts[2])
                elif len(pcts) == 2:
                    ch["retail"]["win_pct"] = float(pcts[0])
                    ch["online"]["win_pct"] = float(pcts[1])
                # If win% shows as "$0.10" (May 2020), skip — derive later

        # Handle + payouts from WAGERS section Total line
        in_wagers = False
        for line in lines:
            if re.search(r'Top\s+\d+\s+Sports\s+by|^WAGERS\b', line, re.I):
                in_wagers = True
                continue
            if in_wagers and re.match(r'\s*Total\b', line.strip(), re.I):
                amounts = self._find_amounts(line)
                if not amounts:
                    continue  # Skip label-only "Total Wagers" lines
                if is_combined:
                    # [Combined_wagers, Total_wagers, Combined_payouts, Total_payouts]
                    if len(amounts) >= 4:
                        ch["total"]["handle"] = amounts[1]
                        ch["total"]["payouts"] = amounts[3]
                    elif len(amounts) >= 2:
                        ch["total"]["handle"] = amounts[-1]
                    else:
                        ch["total"]["handle"] = amounts[0]
                elif len(amounts) >= 6:
                    # R/O/T wagers + R/O/T payouts
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    ch["total"]["handle"] = amounts[2]
                    ch["retail"]["payouts"] = amounts[3]
                    ch["online"]["payouts"] = amounts[4]
                    ch["total"]["payouts"] = amounts[5]
                elif len(amounts) >= 3:
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    ch["total"]["handle"] = amounts[2]
                elif len(amounts) >= 2:
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                elif len(amounts) == 1:
                    ch["retail"]["handle"] = amounts[0]
                break

    def _parse_tabular_post(self, lines: list[str], ch: dict):
        """Post-process tabular OCR with scattered tax/win%/handle values.

        When tabular parser gets GGR/NSBP from metric lines but wagers Total has only
        1 amount, the remaining values (taxes, win%, online/total handles) are scattered
        on separate lines in R → O → T order, separated by labels like "Retail", "Online",
        "WAGERS".
        """
        # Find the Total wagers line (Total| $...)
        total_wagers_idx = None
        in_wagers = False
        for i, line in enumerate(lines):
            if re.search(r'Top\s+\d+\s+Sports\s+by|^WAGERS\b', line.strip(), re.I):
                in_wagers = True
            if in_wagers and re.match(r'\s*Total\s*\|', line.strip(), re.I):
                amounts = self._find_amounts(line)
                if amounts:
                    total_wagers_idx = i
                    if len(amounts) == 1 and ch["retail"]["handle"] == 0:
                        ch["retail"]["handle"] = amounts[0]
                    break

        if total_wagers_idx is None:
            return

        # Find PAYMENTS boundary
        payments_idx = len(lines)
        for i, line in enumerate(lines):
            if re.search(r"PAYMENTS\s+TO\s+PLAYERS", line, re.I):
                payments_idx = i
                break

        # Scan from after Total wagers to PAYMENTS, collecting R → O → T values
        channel_sequence = ["retail", "online", "total"]
        channel_idx = 0

        for i in range(total_wagers_idx + 1, payments_idx):
            text = lines[i].strip()
            if not text:
                continue

            # Labels signal channel transition
            if re.match(r'^(Retail|Online|WAGERS)\s*$', text, re.I):
                if channel_idx < len(channel_sequence) - 1:
                    channel_idx += 1
                continue

            # Win percentages (line with % but no $)
            pcts = re.findall(r'(-?\d+\.\d+)%', text)
            if pcts and '$' not in text:
                chan = channel_sequence[channel_idx]
                if ch[chan]["win_pct"] == 0:
                    ch[chan]["win_pct"] = float(pcts[0])
                continue

            # Dollar amounts
            amounts = self._find_amounts(text)
            if amounts:
                chan = channel_sequence[channel_idx]
                val = amounts[0]
                if val > 10_000_000 and ch[chan]["handle"] == 0:
                    # Large amount = channel handle
                    ch[chan]["handle"] = val
                elif ch[chan]["tax"] == 0:
                    # Smaller amount = tax
                    ch[chan]["tax"] = val

    def _parse_wagers_handle(self, lines: list[str], ch: dict,
                             wagers_idx: int, payments_idx: int, month_idx: int | None):
        """Extract per-channel handle from WAGERS section."""
        if wagers_idx >= len(lines):
            # No WAGERS header found — use fallback strategy
            self._parse_wagers_fallback(lines, ch)
            return

        wagers_end = payments_idx

        # Strategy 1: Look for "Total" line in wagers area with 3+ amounts
        for i in range(wagers_idx, wagers_end):
            line = lines[i].strip()
            if re.match(r'^Total\b', line, re.I):
                amounts = self._find_amounts(line)
                if len(amounts) >= 6:
                    # Tabular: R/O/T wagers + R/O/T payouts
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    ch["total"]["handle"] = amounts[2]
                    ch["retail"]["payouts"] = amounts[3]
                    ch["online"]["payouts"] = amounts[4]
                    ch["total"]["payouts"] = amounts[5]
                    return
                elif len(amounts) >= 3:
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    ch["total"]["handle"] = amounts[2]
                    return
                elif len(amounts) == 2:
                    # Could be retail + online totals (no grand total on this line)
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    # Continue to find grand total
                    break
                elif len(amounts) == 1:
                    # Single amount on Total line = retail handle (in paired-wagers format)
                    # Don't set total — continue to find online and total from other lines
                    ch["retail"]["handle"] = amounts[0]
                    break

        # Strategy 2: Use the month header to find online and total handles
        if month_idx is not None and wagers_idx < month_idx < wagers_end:
            # Forward scan first: find grand total handle (largest amount after month header)
            if ch["total"]["handle"] == 0:
                total_candidates = []
                for j in range(month_idx + 1, wagers_end):
                    amounts = self._find_amounts(lines[j])
                    for a in amounts:
                        if a > 1_000_000:
                            total_candidates.append(a)

                if total_candidates:
                    ch["total"]["handle"] = max(total_candidates)

            # Backward scan: online handle from lines before month header
            # Validate against total handle to avoid picking up sport-level amounts
            if ch["online"]["handle"] == 0:
                for j in range(month_idx - 1, max(wagers_idx, month_idx - 5), -1):
                    amounts = self._find_amounts(lines[j])
                    if amounts:
                        large = [a for a in amounts if a > 1_000_000]
                        candidate = None
                        if len(large) >= 2:
                            ch["retail"]["handle"] = large[-2]
                            candidate = large[-1]
                        elif len(large) == 1:
                            candidate = large[-1]
                        elif amounts:
                            candidate = amounts[-1]
                        # Only trust if >= 25% of total handle (reject sport-level amounts)
                        if candidate is not None:
                            if ch["total"]["handle"] == 0 or candidate >= 0.25 * ch["total"]["handle"]:
                                ch["online"]["handle"] = candidate
                        break

        # Strategy 3: For combined format, look for the single large Total wager
        if ch["total"]["handle"] == 0:
            for line in lines[wagers_idx:wagers_end]:
                stripped = line.strip()
                if re.match(r'^Total\b', stripped, re.I):
                    amounts = self._find_amounts(stripped)
                    if amounts:
                        candidate = max(amounts)
                        if candidate > 10_000_000:
                            ch["total"]["handle"] = candidate

        # Strategy 4: Fallback — search entire text for Total wagers line
        # Also trigger when total is set but both retail and online are missing
        if ch["total"]["handle"] == 0 or (ch["retail"]["handle"] == 0 and ch["online"]["handle"] == 0):
            self._parse_wagers_fallback(lines, ch)

    def _parse_wagers_fallback(self, lines: list[str], ch: dict):
        """Fallback: search entire text for Total wagers line (e.g. 'Total| $R $O $T ...')."""
        for line in lines:
            stripped = line.strip()
            # Match "Total" followed directly by amounts (not "Total GGR" or "Total NSBP")
            if re.match(r'^Total\s*\|?\s*[$]', stripped, re.I):
                amounts = self._find_amounts(stripped)
                if len(amounts) >= 6:
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    ch["total"]["handle"] = amounts[2]
                    ch["retail"]["payouts"] = amounts[3]
                    ch["online"]["payouts"] = amounts[4]
                    ch["total"]["payouts"] = amounts[5]
                    return
                elif len(amounts) >= 3:
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    ch["total"]["handle"] = amounts[2]
                    return
                elif len(amounts) == 2:
                    ch["retail"]["handle"] = amounts[0]
                    ch["online"]["handle"] = amounts[1]
                    return
                elif len(amounts) == 1:
                    ch["retail"]["handle"] = amounts[0]
                    return

    def _derive_channel_values(self, ch: dict, combined_format: bool):
        """Fill in missing channel values from available data."""
        # Cross-channel derivation FIRST (precise values from total - other channel)
        if not combined_format:
            for field in ["ggr", "nsbp", "tax", "handle"]:
                t, r, o = ch["total"][field], ch["retail"][field], ch["online"][field]
                if t != 0 and o != 0 and r == 0:
                    ch["retail"][field] = t - o
                elif t != 0 and r != 0 and o == 0:
                    ch["online"][field] = t - r
                elif t == 0 and (r != 0 or o != 0):
                    ch["total"][field] = r + o

        # Derive handle from GGR + win% if STILL missing (less precise, use as last resort)
        for name in ["retail", "online", "total"]:
            c = ch[name]
            if c["handle"] == 0 and c["ggr"] > 0 and c["win_pct"] > 0:
                c["handle"] = c["ggr"] / (c["win_pct"] / 100)

        # Derive payouts = handle - GGR
        for name in ["retail", "online", "total"]:
            c = ch[name]
            if c["handle"] > 0 and c["ggr"] != 0 and c["payouts"] == 0:
                c["payouts"] = c["handle"] - c["ggr"]

    # ------------------------------------------------------------------
    # Sports parsing
    # ------------------------------------------------------------------
    def _parse_sports(self, text: str) -> dict[str, dict]:
        """Parse sports wager breakdown from Monthly Summary text (per-channel)."""
        lines = text.split("\n")
        sports = self._parse_sports_inline(lines)
        if sports:
            return sports
        return self._parse_sports_columnar(lines)

    def _parse_sports_inline(self, lines: list[str]) -> dict[str, dict]:
        sports = {}
        in_sports = False

        for line in lines:
            stripped = line.strip()
            if re.search(r"top\s+\d+\s+sports", stripped, re.IGNORECASE):
                in_sports = True
                continue
            if not in_sports:
                continue
            if re.match(r"^Total\b", stripped, re.IGNORECASE):
                break
            if re.search(r"^(wagers|payments|retail|online|total wagers)", stripped, re.IGNORECASE) and "$" not in stripped:
                continue

            amounts = self._find_amounts(stripped)
            if not amounts:
                continue

            name_part = re.split(r"\$", stripped)[0].strip()
            if not name_part:
                continue

            canonical = SPORT_NAME_MAP.get(name_part.lower().strip(), name_part)
            if canonical in ("Parlays", "Other"):
                continue

            if len(amounts) >= 6:
                # [retail_wagers, online_wagers, total_wagers, retail_payouts, online_payouts, total_payouts]
                sport_data = {
                    "retail": {"handle": amounts[0], "payouts": amounts[3]},
                    "online": {"handle": amounts[1], "payouts": amounts[4]},
                }
            elif len(amounts) >= 3:
                sport_data = {
                    "retail": {"handle": amounts[0]},
                    "online": {"handle": amounts[1]},
                }
            elif len(amounts) >= 2:
                sport_data = {
                    "retail": {"handle": amounts[0]},
                    "online": {"handle": amounts[1]},
                }
            else:
                sport_data = {
                    "combined": {"handle": amounts[0]},
                }

            if canonical not in sports:
                sports[canonical] = sport_data
            else:
                for ch, data in sport_data.items():
                    if ch not in sports[canonical]:
                        sports[canonical][ch] = data
                    else:
                        for field, val in data.items():
                            sports[canonical][ch][field] = sports[canonical][ch].get(field, 0) + val

        return sports

    def _parse_sports_columnar(self, lines: list[str]) -> dict[str, dict]:
        """Parse sport data from OCR columnar layout.

        Handles two OCR layouts:
        - Layout A: WAGERS → standalone "Online" → single online amounts;
          sport names → standalone "Retail" → single retail amounts
        - Layout B: WAGERS → paired "Retail Online" → paired R/O amounts
          (remaining sports may have single retail-only amounts)

        Derives missing online from total wagers: online = total - retail.
        """
        # --- Step 1: Extract sport names (scan entire text) ---
        sport_names = []
        seen = set()
        for line in lines:
            stripped = line.strip()
            if not stripped or "$" in stripped:
                continue
            if re.search(r"(statewide|summary|taxes|ggr|nsbp|win\s*percentage|"
                         r"proceeds|colorado|betting|percentage of|revised|"
                         r"wagers|payments|retail|online|top\s+\d+|total)", stripped, re.IGNORECASE):
                continue
            if "%" in stripped:
                continue

            canonical = SPORT_NAME_MAP.get(stripped.lower().strip())
            if canonical and canonical not in seen:
                sport_names.append(canonical)
                seen.add(canonical)

        filtered = [n for n in sport_names if n not in ("Parlays", "Other")]
        if not filtered:
            return {}
        max_entries = len(sport_names)  # includes Parlays/Other for column counting

        # --- Step 2: Extract wagers from WAGERS section ---
        retail_wagers = []
        online_wagers = []
        wagers_layout = None

        for i, line in enumerate(lines):
            stripped = line.strip()
            if not re.search(r'^WAGERS\b', stripped, re.I):
                continue

            for j in range(i + 1, min(i + 4, len(lines))):
                hdr = lines[j].strip()
                if re.match(r'^\s*Retail\s+Online\s*$', hdr, re.I):
                    # Layout B: paired R/O amounts
                    wagers_layout = "paired"
                    for k in range(j + 1, len(lines)):
                        s = lines[k].strip()
                        if not s:
                            continue
                        if re.match(r'^\s*Total', s, re.I):
                            break
                        if len(retail_wagers) >= max_entries:
                            break
                        amounts = self._find_amounts(s)
                        if len(amounts) >= 2:
                            retail_wagers.append(amounts[0])
                            online_wagers.append(amounts[1])
                        elif len(amounts) == 1:
                            retail_wagers.append(amounts[0])
                            online_wagers.append(0)  # placeholder
                    break
                elif re.match(r'^\s*Online\s*$', hdr, re.I):
                    # Layout A: single online column
                    wagers_layout = "online_only"
                    for k in range(j + 1, len(lines)):
                        s = lines[k].strip()
                        if not s:
                            continue
                        if re.match(r'^\s*Total', s, re.I):
                            break
                        if re.search(r'PAYMENTS', s, re.I):
                            break
                        if len(online_wagers) >= max_entries:
                            break
                        amounts = self._find_amounts(s)
                        if len(amounts) == 1:
                            online_wagers.append(amounts[0])
                        elif len(amounts) > 1:
                            break
                    break
            break

        # --- Step 3: Extract retail wagers for Layout A ---
        if wagers_layout == "online_only":
            sport_section_idx = None
            for i, line in enumerate(lines):
                if re.search(r'top\s+\d+\s+sports', line, re.I):
                    sport_section_idx = i

            if sport_section_idx is not None:
                retail_start = None
                for i in range(sport_section_idx, len(lines)):
                    if re.match(r'^\s*Retail\s*$', lines[i].strip(), re.I):
                        retail_start = i
                        break

                if retail_start is not None:
                    for i in range(retail_start + 1, len(lines)):
                        stripped = lines[i].strip()
                        if not stripped:
                            continue
                        if len(retail_wagers) >= max_entries:
                            break
                        amounts = self._find_amounts(stripped)
                        if not amounts:
                            break
                        if len(amounts) == 1:
                            retail_wagers.append(amounts[0])
                        else:
                            break

        # --- Step 4: Extract total wagers from month header section ---
        total_wagers = []
        month_idx = None
        for i, line in enumerate(lines):
            if re.search(rf'(?:{MONTH_PATTERN})\s+20\d{{2}}', line, re.I):
                month_idx = i

        if month_idx is not None:
            post_amounts = []
            for i in range(month_idx + 1, len(lines)):
                stripped = lines[i].strip()
                if not stripped:
                    continue
                if re.search(r'Percentage\s+of', stripped, re.I):
                    break
                amounts = self._find_amounts(stripped)
                for a in amounts:
                    post_amounts.append(a)

            # Skip first 3 summary values (GGR, NSBP, Tax totals)
            if len(post_amounts) > 3:
                total_wagers = post_amounts[3:]

        # --- Step 5: Derive missing online = total - retail ---
        num_sports = len(filtered)
        for i in range(num_sports):
            o = online_wagers[i] if i < len(online_wagers) else 0
            r = retail_wagers[i] if i < len(retail_wagers) else 0
            t = total_wagers[i] if i < len(total_wagers) else 0

            if o == 0 and t > 0:
                derived = t - r
                if derived > 0:
                    while len(online_wagers) <= i:
                        online_wagers.append(0)
                    online_wagers[i] = derived

        # --- Step 6: Extract R/O payouts from PAYMENTS section ---
        retail_payouts = []
        online_payouts = []
        in_payouts = False
        for line in lines:
            stripped = line.strip()
            if re.search(r'PAYMENTS\s+TO\s+PLAYERS', stripped, re.I):
                in_payouts = True
                continue
            if in_payouts:
                if not stripped:
                    continue
                if re.match(r'^\s*Retail\s+Online\s*$', stripped, re.I):
                    continue
                if re.search(r'top\s+\d+\s+sports', stripped, re.I):
                    break
                amounts = self._find_amounts(stripped)
                if len(amounts) >= 2:
                    retail_payouts.append(amounts[0])
                    online_payouts.append(amounts[1])

        # --- Step 7: Build per-channel sport dict ---
        sports = {}
        for i, name in enumerate(filtered):
            sport_data = {}
            r_handle = retail_wagers[i] if i < len(retail_wagers) else 0
            o_handle = online_wagers[i] if i < len(online_wagers) else 0
            r_payout = retail_payouts[i] if i < len(retail_payouts) else 0
            o_payout = online_payouts[i] if i < len(online_payouts) else 0

            if r_handle > 0 or r_payout > 0:
                rd = {}
                if r_handle > 0:
                    rd["handle"] = r_handle
                if r_payout > 0:
                    rd["payouts"] = r_payout
                sport_data["retail"] = rd
            if o_handle > 0 or o_payout > 0:
                od = {}
                if o_handle > 0:
                    od["handle"] = o_handle
                if o_payout > 0:
                    od["payouts"] = o_payout
                sport_data["online"] = od

            if sport_data:
                sports[name] = sport_data

        return sports

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------
    @staticmethod
    def _find_amounts(text: str) -> list[float]:
        """Extract all dollar amounts from a string. Handles negatives and OCR artifacts."""
        raw = re.findall(
            r'(?:'
            r'\(?\-?\$\s*[\d,]+(?:\.\d+)?\)?'       # Standard: $1,234.56, -$1,234.56, ($1,234.56)
            r'|'
            r'\$\s*\(\s*[\d,]+(?:\.\d+)?\s*\)'       # Space-paren: $ (1,234.56)
            r')',
            text
        )
        results = []
        for s in raw:
            neg = '(' in s or (s.startswith('-') and '$' in s)
            cleaned = re.sub(r'[$,\s()\-]', '', s)
            try:
                v = float(cleaned)
                if neg and v > 0:
                    v = -v
                results.append(v)
            except ValueError:
                pass
        return results

    def _parse_money(self, value) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if s.startswith("(") and ")" in s:
            s = "-" + s.replace("(", "").replace(")", "")
        s = s.replace("$", "").replace(",", "").strip()
        if not s or s in ("-", "N/A", ""):
            return None
        try:
            return float(s)
        except ValueError:
            return None


def _safe_val(v):
    """Safely convert a value that might be pd.NA to a float."""
    if pd.isna(v):
        return 0
    return float(v)


if __name__ == "__main__":
    scraper = COScraper()
    df = scraper.run(backfill=True)
    if not df.empty:
        print(f"\n{'='*60}")
        print(f"CO SCRAPER RESULTS")
        print(f"{'='*60}")
        print(f"Total rows: {len(df)}")
        print(f"Date range: {df['period_end'].min()} to {df['period_end'].max()}")
        print(f"Period types: {df['period_type'].value_counts().to_dict()}")
        if 'channel' in df.columns:
            print(f"Channels: {df['channel'].value_counts().to_dict()}")
        if 'sport_category' in df.columns:
            sports = df[df['sport_category'].notna()]
            print(f"Sport rows: {len(sports)}")

        for col in ['handle', 'gross_revenue', 'standard_ggr', 'payouts',
                    'promo_credits', 'net_revenue', 'tax_paid']:
            if col in df.columns:
                nonnull = df[col].notna().sum()
                nonzero = (df[col].notna() & (df[col] != 0)).sum()
                print(f"  {col}: {nonnull}/{len(df)} non-null, {nonzero} non-zero")

        print(f"\nRecent periods (per channel):")
        agg = df[df['sport_category'].isna()] if 'sport_category' in df.columns else df
        for pe in sorted(agg['period_end'].unique())[-6:]:
            rows = agg[agg['period_end'] == pe]
            for _, row in rows.iterrows():
                ch = row.get('channel', '?')
                h = _safe_val(row.get('handle', 0))
                g = _safe_val(row.get('gross_revenue', 0))
                nr = _safe_val(row.get('net_revenue', 0))
                sg = _safe_val(row.get('standard_ggr', 0))
                print(f"  {pe} [{ch:8s}]: handle=${h/100:>14,.0f}  GGR=${g/100:>12,.0f}  StdGGR=${sg/100:>12,.0f}  NSBP=${nr/100:>12,.0f}")
    else:
        print("No data scraped.")

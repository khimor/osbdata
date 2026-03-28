"""
Shared helpers for state sports betting research.
"""
import os
import json
import time
import requests
import traceback
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

RESEARCH_DIR = Path("/Users/nosherzapoo/Desktop/claude/osb-trackerv0/research")
SAMPLES_DIR = RESEARCH_DIR / "samples"
SCREENSHOTS_DIR = RESEARCH_DIR / "screenshots"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def ensure_state_dir(state_code):
    d = SAMPLES_DIR / state_code
    d.mkdir(parents=True, exist_ok=True)
    return d

def fetch_with_requests(url, state_code, timeout=30):
    """Fetch a URL with requests. Returns (html_text, response) or (None, None)."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        state_dir = ensure_state_dir(state_code)
        with open(state_dir / "page.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        return resp.text, resp
    except Exception as e:
        return None, None

def fetch_with_playwright(url, state_code, timeout=30000):
    """Fetch a URL with Playwright. Returns (html_text, page) or (None, None). Caller must manage browser lifecycle."""
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = context.new_page()
        page.goto(url, timeout=timeout, wait_until="networkidle")
        time.sleep(2)  # Let JS render

        # Screenshot
        screenshot_path = SCREENSHOTS_DIR / f"{state_code}.png"
        page.screenshot(path=str(screenshot_path), full_page=True)

        # Save HTML
        html = page.content()
        state_dir = ensure_state_dir(state_code)
        with open(state_dir / "page.html", "w", encoding="utf-8") as f:
            f.write(html)

        browser.close()
        pw.stop()
        return html, None
    except Exception as e:
        try:
            browser.close()
            pw.stop()
        except:
            pass
        return None, None

def download_file(url, state_code, filename=None, timeout=60):
    """Download a file to the state's samples directory. Returns the saved path or None."""
    try:
        if not filename:
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path)
            if not filename or len(filename) < 3:
                filename = "download"

        state_dir = ensure_state_dir(state_code)
        save_path = state_dir / filename

        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        resp.raise_for_status()

        with open(save_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return str(save_path)
    except Exception as e:
        return None

def find_download_links(html, base_url, extensions=None):
    """Find all download links in HTML page. Returns list of (url, filename, extension)."""
    if extensions is None:
        extensions = ['.xlsx', '.xls', '.csv', '.pdf', '.zip']

    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        path = parsed.path.lower()

        for ext in extensions:
            if path.endswith(ext):
                filename = os.path.basename(parsed.path)
                links.append((full_url, filename, ext))
                break

    return links

def parse_excel(filepath):
    """Parse an Excel file and return summary info."""
    import pandas as pd
    try:
        xls = pd.ExcelFile(filepath)
        result = {
            "sheet_names": xls.sheet_names,
            "sheets": {}
        }
        for sheet in xls.sheet_names:
            df = pd.read_excel(filepath, sheet_name=sheet)
            result["sheets"][sheet] = {
                "columns": list(df.columns),
                "row_count": len(df),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "sample_rows": df.head(5).to_dict(orient="records")
            }
        return result
    except Exception as e:
        return {"error": str(e)}

def parse_csv(filepath):
    """Parse a CSV file and return summary info."""
    import pandas as pd
    try:
        df = pd.read_csv(filepath)
        return {
            "columns": list(df.columns),
            "row_count": len(df),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "sample_rows": df.head(5).to_dict(orient="records")
        }
    except Exception as e:
        return {"error": str(e)}

def parse_pdf(filepath):
    """Parse a PDF file and return summary info."""
    import pdfplumber
    try:
        with pdfplumber.open(filepath) as pdf:
            result = {
                "page_count": len(pdf.pages),
                "pages": []
            }
            for i, page in enumerate(pdf.pages[:5]):  # First 5 pages only
                text = page.extract_text() or ""
                tables = page.extract_tables() or []
                page_info = {
                    "page_num": i + 1,
                    "text_preview": text[:1000],
                    "table_count": len(tables),
                    "tables": []
                }
                for t in tables[:3]:  # First 3 tables per page
                    if t and len(t) > 0:
                        page_info["tables"].append({
                            "headers": t[0] if t else [],
                            "row_count": len(t) - 1,
                            "sample_rows": t[1:4] if len(t) > 1 else []
                        })
                result["pages"].append(page_info)
            return result
    except Exception as e:
        return {"error": str(e)}

def parse_html_tables(html):
    """Parse HTML tables and return summary info."""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    result = []
    for i, table in enumerate(tables[:10]):  # First 10 tables
        headers = []
        for th in table.find_all("th"):
            headers.append(th.get_text(strip=True))

        rows = []
        for tr in table.find_all("tr")[:6]:  # First 5 data rows + header
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(cells)

        result.append({
            "table_index": i,
            "headers": headers,
            "row_count": len(table.find_all("tr")),
            "sample_rows": rows[:6]
        })
    return result

def log_entry(state_code, message):
    """Append a log entry."""
    timestamp = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    log_path = RESEARCH_DIR / "research_log.md"
    with open(log_path, "a") as f:
        f.write(f"\n### {state_code} — {timestamp}\n")
        f.write(message + "\n")

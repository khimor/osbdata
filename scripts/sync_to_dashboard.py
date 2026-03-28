"""
Sync processed CSVs and screenshots to the dashboard public directory.
Run after scraping to prepare data for the frontend.
"""

import shutil
from pathlib import Path

ROOT = Path(__file__).parent.parent
PROCESSED = ROOT / "data" / "processed"
RAW = ROOT / "data" / "raw"
PUBLIC_DATA = ROOT / "dashboard" / "public" / "data"
PUBLIC_SOURCES = ROOT / "dashboard" / "public" / "sources"


def sync_csvs():
    """Copy all state CSVs to dashboard public/data."""
    PUBLIC_DATA.mkdir(parents=True, exist_ok=True)
    count = 0
    for csv in PROCESSED.glob("??.csv"):
        dest = PUBLIC_DATA / csv.name
        if not dest.exists() or csv.stat().st_mtime > dest.stat().st_mtime:
            shutil.copy2(csv, dest)
            count += 1
    return count


def sync_screenshots():
    """Copy new screenshots to dashboard public/sources."""
    count = 0
    for state_dir in RAW.iterdir():
        ss_dir = state_dir / "screenshots"
        if not ss_dir.is_dir():
            continue
        dest = PUBLIC_SOURCES / state_dir.name / "screenshots"
        dest.mkdir(parents=True, exist_ok=True)
        for png in ss_dir.glob("*.png"):
            dest_file = dest / png.name
            if not dest_file.exists():
                shutil.copy2(png, dest_file)
                count += 1
    return count


def main():
    csv_count = sync_csvs()
    ss_count = sync_screenshots()
    print(f"Synced {csv_count} CSV(s), {ss_count} screenshot(s) to dashboard")


if __name__ == '__main__':
    main()

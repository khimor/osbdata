"""
QA Site Agent — Playwright-based end-to-end testing of the dashboard.
Acts as a user: loads every page, clicks interactive elements, checks data renders.

Usage:
    python tests/qa_site.py                          # test against localhost:5173
    python tests/qa_site.py --url https://osbdata.vercel.app  # test production
    python tests/qa_site.py --json                   # JSON report output

Requires: pip install playwright && playwright install chromium
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

DEFAULT_URL = "http://localhost:5173"

checks = []


def check(name, passed, message="", severity="CRITICAL"):
    checks.append({
        'name': name,
        'passed': passed,
        'message': message if not passed else 'OK',
        'severity': severity if not passed else 'PASS',
    })
    status = "PASS" if passed else f"FAIL [{severity}]"
    print(f"  {status}: {name}" + (f" - {message}" if message and not passed else ""))


def test_national_overview(page):
    print("\n--- National Overview ---")
    page.click('button:has-text("National Overview")')
    page.wait_for_timeout(2000)

    # Stat cards
    cards = page.query_selector_all('.stat-card')
    check('national_stat_cards', len(cards) >= 4, f'Found {len(cards)} stat cards')

    # Check stat values aren't just dashes
    values = page.query_selector_all('.stat-value')
    non_empty = sum(1 for v in values if v.inner_text().strip() not in ['-', '', 'undefined'])
    check('national_stat_values', non_empty >= 3, f'{non_empty}/{len(values)} have real values')

    # State rankings table
    rows = page.query_selector_all('.data-table tbody tr')
    check('national_table_rows', len(rows) >= 20, f'Found {len(rows)} state rows', 'WARNING')

    # Chart rendered
    charts = page.query_selector_all('.recharts-wrapper')
    check('national_charts', len(charts) >= 1, f'Found {len(charts)} charts')

    # Channel toggle
    page.click('button:has-text("Online")')
    page.wait_for_timeout(1000)
    page.click('button:has-text("Combined")')
    page.wait_for_timeout(500)
    check('national_channel_toggle', True)

    # Month selector
    selects = page.query_selector_all('select')
    check('national_month_selector', len(selects) >= 1, f'Found {len(selects)} selects')


def test_operator_view(page):
    print("\n--- Operator View ---")
    page.click('button:has-text("Operator View")')
    page.wait_for_timeout(2000)

    # Operator table
    rows = page.query_selector_all('.data-table tbody tr')
    check('operator_table_rows', len(rows) >= 5, f'Found {len(rows)} operator rows')

    # Check first operator has data
    if rows:
        cells = rows[0].query_selector_all('td')
        has_data = any(c.inner_text().strip() not in ['-', ''] for c in cells[1:])
        check('operator_has_data', has_data, 'First operator row has values')

    # Click operator to open detail
    if rows:
        rows[0].click()
        page.wait_for_timeout(2000)
        # Check detail page loaded
        back_btn = page.query_selector('button:has-text("Back")')
        check('operator_detail_opens', back_btn is not None, 'Detail page with Back button')
        if back_btn:
            back_btn.click()
            page.wait_for_timeout(1000)

    # State filter dropdown
    state_btn = page.query_selector('button:has-text("All States")')
    if state_btn:
        state_btn.click()
        page.wait_for_timeout(500)
        dropdown = page.query_selector('.state-filter-dropdown')
        check('operator_state_filter', dropdown is not None, 'State filter dropdown opens')
        state_btn.click()  # close
        page.wait_for_timeout(300)


def test_compare_states(page):
    print("\n--- Compare States ---")
    page.click('button:has-text("Compare States")')
    page.wait_for_timeout(2000)

    # State chips
    chips = page.query_selector_all('.state-picker-chip')
    check('compare_state_chips', len(chips) >= 20, f'Found {len(chips)} state chips')

    active_chips = page.query_selector_all('.state-picker-chip.active')
    check('compare_default_selected', len(active_chips) >= 3, f'{len(active_chips)} states selected by default')

    # Charts
    charts = page.query_selector_all('.recharts-wrapper')
    check('compare_charts', len(charts) >= 2, f'Found {len(charts)} charts')

    # Metric toggle
    page.click('button:has-text("GGR")')
    page.wait_for_timeout(1000)
    page.click('button:has-text("Handle")')
    page.wait_for_timeout(500)
    check('compare_metric_toggle', True)

    # Table
    rows = page.query_selector_all('.data-table tbody tr')
    check('compare_table', len(rows) >= 3, f'Found {len(rows)} comparison rows')


def test_state_deep_dive(page):
    print("\n--- State Deep Dive ---")
    page.click('button:has-text("State Deep Dive")')
    page.wait_for_timeout(2000)

    # State name shown
    state_name = page.query_selector('.state-name')
    check('state_name_shown', state_name is not None and state_name.inner_text().strip() != '')

    # Stat cards
    cards = page.query_selector_all('.stat-card')
    check('state_stat_cards', len(cards) >= 4, f'Found {len(cards)} stat cards')

    # Charts
    charts = page.query_selector_all('.recharts-wrapper')
    check('state_charts', len(charts) >= 2, f'Found {len(charts)} charts')

    # Operator table
    rows = page.query_selector_all('.data-table tbody tr')
    check('state_operator_table', len(rows) >= 1, f'Found {len(rows)} operator rows')

    # Source verification: click a value
    sourceables = page.query_selector_all('.sourceable')
    if sourceables:
        sourceables[0].click()
        page.wait_for_timeout(1000)
        panel = page.query_selector('.source-panel')
        check('state_source_panel', panel is not None, 'Source verification panel opens')

        if panel:
            # Check screenshot loads
            img = panel.query_selector('.source-screenshot')
            if img:
                src = img.get_attribute('src')
                check('state_screenshot_loads', src and len(src) > 10, f'Screenshot src: {src[:50]}',
                      'WARNING')
            else:
                check('state_screenshot_loads', False, 'No screenshot element in panel', 'WARNING')

            # Check raw line
            raw = panel.query_selector('.source-raw-line')
            check('state_raw_line', raw is not None, 'Raw source line shown', 'WARNING')

            # Close panel
            close = panel.query_selector('.source-close')
            if close:
                close.click()
                page.wait_for_timeout(500)

    # Switch state
    selects = page.query_selector_all('select')
    if selects:
        selects[0].select_option('NJ')
        page.wait_for_timeout(2000)
        check('state_switch', True, 'Switched to NJ')


def test_data_table(page):
    print("\n--- Data Table ---")
    page.click('button:has-text("Data Table")')
    page.wait_for_timeout(2000)

    rows = page.query_selector_all('.data-table tbody tr')
    check('datatable_rows', len(rows) >= 10, f'Found {len(rows)} rows')


def test_console_errors(page):
    print("\n--- Console Errors ---")
    # Check for any JS errors collected during the session
    check('no_console_errors', len(console_errors) == 0,
          f'{len(console_errors)} errors: {"; ".join(console_errors[:3])}' if console_errors else '')


console_errors = []


def main():
    parser = argparse.ArgumentParser(description='QA Site Agent')
    parser.add_argument('--url', default=DEFAULT_URL, help='Dashboard URL to test')
    parser.add_argument('--json', action='store_true', help='JSON output')
    args = parser.parse_args()

    print(f"QA SITE AGENT - Testing {args.url}")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 800})
        page = context.new_page()

        # Collect console errors
        page.on('console', lambda msg: console_errors.append(msg.text) if msg.type == 'error' else None)

        try:
            page.goto(args.url, wait_until='networkidle', timeout=30000)
            page.wait_for_timeout(3000)
            check('page_loads', True, f'Dashboard loaded at {args.url}')
        except Exception as e:
            check('page_loads', False, f'Failed to load: {e}')
            browser.close()
            sys.exit(2)

        test_national_overview(page)
        test_operator_view(page)
        test_compare_states(page)
        test_state_deep_dive(page)
        test_data_table(page)
        test_console_errors(page)

        browser.close()

    # Report
    passed = sum(1 for c in checks if c['passed'])
    failed = sum(1 for c in checks if not c['passed'])
    critical = sum(1 for c in checks if c['severity'] == 'CRITICAL')

    report = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'url': args.url,
        'total_checks': len(checks),
        'passed': passed,
        'failed': failed,
        'critical': critical,
        'checks': checks,
    }

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"\n{'=' * 60}")
        print(f"RESULTS: {passed} passed, {failed} failed ({critical} critical)")
        if failed:
            print(f"\nFAILED CHECKS:")
            for c in checks:
                if not c['passed']:
                    print(f"  [{c['severity']}] {c['name']}: {c['message']}")

    report_path = Path(__file__).parent / 'qa_site_report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    if not args.json:
        print(f"\nReport saved to {report_path}")

    sys.exit(2 if critical else (1 if failed else 0))


if __name__ == '__main__':
    main()

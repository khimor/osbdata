# Skill: Building a State Sports Betting Scraper

## When to use
Use this skill any time you are building a new state scraper, modifying an existing one, or debugging a data issue with a state's pipeline.

## Architecture
Every state scraper is a single Python file in `scrapers/{state_code}_scraper.py` that inherits from `BaseStateScraper` in `scrapers/base_scraper.py`. The base class handles logging, normalization, validation, and DB upsert. You only implement three methods:

1. `discover_periods()` — Find all available reporting periods dynamically
2. `download_report(period_info)` — Download the raw file and save to `data/raw/{state_code}/`
3. `parse_report(file_path, period_info)` — Parse into a DataFrame with STANDARD_COLUMNS

## Standard Columns (every parse_report must output these)
Required: period_start, period_end, period_type, operator_raw, handle
Optional but include when available: gross_revenue, promo_credits, net_revenue, payouts, tax_paid, hold_pct, federal_excise_tax, channel, sport_category

The base class adds: state_code, operator_standard, parent_company, scrape_timestamp, source_file, days_in_period, is_partial_period

## Currency
ALL money values in the DataFrame should be in DOLLARS (float) when returned from parse_report(). The base class converts to cents (integer) during normalization. Do NOT convert to cents yourself.

## Operator Names
Return the raw operator name exactly as it appears in the source report in `operator_raw`. The base class calls `normalize_operator()` to populate `operator_standard`. If you see a name not in the mapping, add it to `scrapers/operator_mapping.py`.

## Sport Categories
Return the raw sport name in `sport_category`. The base class normalizes it via `normalize_sport()`. If the state doesn't break down by sport, don't include the column — the base class will set it to NULL.

## Weekly vs Monthly Data
If a state reports weekly data (e.g., NY, WV), store the weekly rows with `period_type='weekly'`. ALSO compute and store monthly aggregations with `period_type='monthly'` by summing handle/revenue/promo/tax and averaging hold_pct weighted by handle. Both granularities live in the same table. Use the `_aggregate_to_monthly()` method in the base class.

## Channel
If the state splits retail vs online, store separate rows with `channel='online'` and `channel='retail'`. Also store a `channel='combined'` total row. If the state doesn't split, use `channel='combined'` only.

## Validation Checklist (run after every parse)
- [ ] Operator rows sum to ±1% of state total row (if total exists in source)
- [ ] Handle > 0 for all rows (except rare operator exit months)
- [ ] GGR ≤ Handle (hold% ≤ 100%)
- [ ] No future dates
- [ ] No duplicate (period_end, operator, channel, sport) combinations
- [ ] Currency units are consistent (not mixing $ and $thousands)
- [ ] Cross-check the most recent month against a third-party source

## 403 / Blocked Site Protocol
1. Try requests with rotating User-Agent
2. Try adding Referer header matching the site's domain
3. Try Playwright with stealth mode (headless=True)
4. Try Playwright with headless=False if headless fails
5. Try fetching Google's cached version: `https://webcache.googleusercontent.com/search?q=cache:{url}`
6. Try searching for direct file download URLs via web search
7. If all fail, log it and move on — but ALWAYS try all 6 approaches first

## File Naming
Raw files: `data/raw/{STATE_CODE}/{STATE_CODE}_{YYYY_MM}.{ext}` (e.g., `data/raw/NY/NY_2024_01.xlsx`)
For weekly: `data/raw/{STATE_CODE}/{STATE_CODE}_{YYYY_MM_DD}.{ext}`
For operators with separate files: `data/raw/{STATE_CODE}/{STATE_CODE}_{operator}_{YYYY_MM}.{ext}`

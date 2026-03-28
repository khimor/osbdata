# Skill: Sports Betting Data Validation

## When to use
Use this skill after parsing any state's data, when debugging data quality issues, or when a user reports numbers that look wrong.

## Cross-Check Methodology

### Internal Consistency Checks
For every parsed DataFrame:
1. **Sum check**: If source has a "Total" row, verify SUM(operator rows) = Total row (±1%)
2. **GGR formula check**: If handle and payouts are both available, verify handle - payouts ≈ gross_revenue (±2%)
3. **Net revenue check**: gross_revenue - promo_credits ≈ net_revenue (±1%)
4. **Tax check**: net_revenue × known_tax_rate ≈ tax_paid (±5%, allow more slack due to graduated rates)
5. **Hold% sanity**: hold_pct should be between 0.03 and 0.25 for most months. Flag anything outside 0.01-0.40.
6. **Handle magnitude**: For top-10 states, monthly handle should be $500M-$2B. If you see $500K, you probably parsed thousands not dollars. If you see $500B, you probably have a unit issue.

### External Cross-Checks
After scraping a state, cross-reference the most recent available month against at least ONE of:
- rg.org/statistics/us/{state}
- covers.com/betting/usa/{state}/betting-revenue-tracker
- playstatename.com/sports-betting/revenue/ (e.g., playnj.com, playpennsylvania.com)
- Legal Sports Report / SportsHandle articles

The cross-check doesn't need to be automated — just search for "{state} sports betting revenue {month} {year}" and compare the headline number to what you parsed. If they differ by more than 5%, investigate.

### State-Specific Gotchas
- **NY**: GGR is BEFORE promo deductions. Tax is on gross, not net. Some sources report "tax revenue" which is GGR × 51%.
- **NJ**: Handle per operator is NOT reported — only revenue. If you see per-operator handle, it's from a third-party estimate, not official.
- **TN**: Taxes HANDLE, not GGR. The "privilege tax" column = handle × 1.85%. If you compare TN tax/revenue to other states without noting this, your analysis is wrong.
- **OH**: Revenue can be ZERO for a month (losses don't carry forward). A $0 revenue row is real, not missing data.
- **AZ**: Promo deduction cap decreases each year from launch. The GGR number in older reports may have different promo treatment than newer ones.
- **PA**: Reports by casino property AND by individual skin/brand. Don't double-count.
- **MI**: Tribal operators report online data to MGCB but NOT retail data. Retail tribal data is a known gap.
- **IL**: Graduated tax means different operators pay different effective rates. Don't assume a single rate.

### Data Freshness
Most states publish with a 2-6 week lag. If the latest available data is from 2+ months ago, that's normal — not a scraper bug.

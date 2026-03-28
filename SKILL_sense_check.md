# Skill: Sense Check Scraper Output

## When to use
Run after any scraper produces data — before validation and promotion. This catches bugs where the scraper outputs structurally valid CSVs with wrong numbers: unit errors, column mixups, YTD parsed as monthly, and other logic bugs that pass schema validation but produce garbage.

## Quick Start

```bash
python3 -m pipeline.sense_check              # all states
python3 -m pipeline.sense_check NY PA CT     # specific states
```

## What It Catches

### SUSPECT (likely real bugs)

| Check | What it detects | Example |
|---|---|---|
| `column_swap` | handle == gross_revenue (column duplication) or gross_revenue > handle consistently (swap) | MO: handle equals gross_revenue in 100% of rows |
| `unit_error` | Values off by 100x or 1000x from expected magnitude, or tax/handle ratio is impossible | MT: median monthly handle $36 (should be $millions) |
| `ytd_as_monthly` | Year-to-date accumulation parsed as single months — last month is 5x+ previous, or monotonic increase within calendar year | DE: Feb value is 7.6x January across all operators |
| `mom_spike` | State-wide handle jumps or drops >5x between months (not launch-related) | KS: handle drops to near-zero every July (fiscal year reset) |
| `stale_data` | Same exact handle value repeated 4+ consecutive months (copy-paste) | Detected per-operator |
| `cross_field` | net_revenue > gross_revenue in >50% of rows (columns likely swapped) | IN: net > gross in 76% of rows |

### REVIEW (needs human judgment)

| Check | What it detects |
|---|---|
| `double_count` | Both TOTAL + operator rows, or combined + online/retail channels coexist |
| `cross_field` | promo_credits > 1.5x gross_revenue, or tax_paid > net_revenue in some rows |
| `operator_stability` | Operator count drops >60% in a month (suggests incomplete parse) |
| `hold_stability` | State-wide hold% fluctuates >3x from median (signals unit inconsistency) |
| `mom_spike` | 1-2 month spikes (could be launch or seasonal, not necessarily a bug) |

## How to Read Results

```
X [ytd_as_monthly] 4 operator(s) show YTD accumulation pattern
    Delaware Park: last month 7.6x previous ($106M vs $14M)
```

- **X** = SUSPECT (likely a real scraper bug)
- **?** = REVIEW (needs human judgment)
- **.** = OK (check passed)

## Current State of All Scrapers

**12 states with suspected bugs (as of 2026-03-23):**

| State | Bug | Root Cause |
|---|---|---|
| **NH** | column_swap, unit_error, ytd_as_monthly, mom_spike | Handle and gross_revenue appear to be in different units; data has wild inconsistencies |
| **MT** | column_swap, unit_error, ytd_as_monthly | Handle values are tiny ($36/month); gross_revenue > handle; likely unit/parsing issue |
| **MO** | column_swap, ytd_as_monthly | Scraper puts handle into gross_revenue column; some operators show YTD pattern |
| **IN** | ytd_as_monthly, cross_field | net_revenue > gross_revenue in 76% of rows (swap?); some operators accumulate YTD |
| **KS** | ytd_as_monthly, mom_spike | Handle drops to near-zero every July then builds up — classic fiscal year YTD |
| **RI** | ytd_as_monthly, mom_spike | Early years (2011-2015) have very small handle with huge hold% outliers |
| **AZ** | ytd_as_monthly | 3 operators show last-month spike (BetMGM 255x, Caesars 40x, DraftKings 54x) |
| **DC** | ytd_as_monthly | FanDuel Audi Field in 2022 shows monotonic YTD increase |
| **DE** | ytd_as_monthly | Feb 2026 is clearly YTD across all 4 operators (7-10x spike) |
| **MD** | ytd_as_monthly | Maryland Stadium Mobile in 2023 shows 1471x growth pattern |
| **PA** | ytd_as_monthly | 8 operators show last-month spikes (up to 184x) |
| **SD** | ytd_as_monthly | Deadwood Casinos last month 191x previous |

**23 clean states:** AR, CO, CT, IA, IL, KY, LA, MA, ME, MI, MS, NC, NE, NJ, NV, NY, OH, OR, TN, VA, VT, WV, WY

## Python API

```python
from pipeline.sense_check import SenseChecker, sense_check_state, sense_check_all

result = sense_check_state("MO")
print(result.clean)       # False
print(result.suspects)    # [Finding(column_swap, ...), Finding(ytd_as_monthly, ...)]
print(result.reviews)     # []

# All states
results = sense_check_all()
```

## Relationship to Other Skills

```
Scraper runs  →  sense_check (catches scraper bugs)
              →  validate_and_promote (checks schema + consistency)
              →  promote to dashboard
```

Run sense_check FIRST. If it finds SUSPECT issues, fix the scraper before running validation. Validation catches data quality issues; sense_check catches scraper logic bugs.

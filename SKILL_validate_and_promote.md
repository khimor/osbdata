# Skill: Validate and Promote State Data

## When to use
Use this skill after scraping new data for any state, before considering it "in the system." Run validation to catch data quality issues, then promote passing states to the dashboard.

## Quick Start

```bash
# Validate all states
python3 -m pipeline.validate_and_promote

# Validate specific states
python3 -m pipeline.validate_and_promote NY PA CT

# Validate + promote passing states to dashboard
python3 -m pipeline.validate_and_promote --promote

# Validate + fix missing standard_ggr + promote
python3 -m pipeline.validate_and_promote --promote --fix
```

## What It Checks

### Blocking Errors (prevent promotion)
| Check | What it catches |
|---|---|
| `schema` | Missing required columns (state_code, period_end, period_type, operator_standard, channel) |
| `empty` | No data rows at all |
| `types` | Non-numeric values in money columns |
| `duplicates` | Duplicate rows on composite key (state, period, operator, channel, sport) |
| `date_range` | Future dates in period_end |
| `negative_handle` | >5% of rows with negative handle (small counts are normal adjustments) |
| `handle_magnitude` | Handle impossibly high for the state's tier |

### Warnings (logged, do not block)
| Check | What it catches |
|---|---|
| `date_range` | Data before state launch date |
| `date_gaps` | Missing months in monthly reporting |
| `channels` | Config says channel split available but only 'combined' in data |
| `hold_sanity` | Hold% outside 1-40% or negative |
| `ggr_consistency` | handle - payouts != gross_revenue (>2% diff) |
| `net_revenue_consistency` | gross_revenue - promo_credits != net_revenue (>1% diff) |
| `tax_consistency` | tax_paid differs from revenue x rate by >10% |
| `negative_handle` | <=5% negative handle rows (normal adjustments) |
| `operator_normalization` | Unmapped operators (raw == standard) |
| `standard_ggr` | Column missing or all null |

### Info (context only)
| Check | What it reports |
|---|---|
| `channels` | Channel breakdown (online/retail/combined counts) |
| `completeness` | Which money fields are populated vs absent |
| `standard_ggr` | Partial null count |

## Data Flow

```
Scraper → data/raw/{STATE}/        (raw downloads)
       → data/processed/{STATE}.csv (normalized, in cents)

Validator reads data/processed/{STATE}.csv
  ├── PASS → promote to dashboard/dist/data/{STATE}.csv
  └── FAIL → fix issues, re-scrape, or re-validate
```

## Promotion

When `--promote` is passed:
1. Validates each state
2. Passing states: copies processed CSV to `dashboard/dist/data/`
3. Ensures all 23 STANDARD_COLUMNS are present in correct order

When `--fix` is also passed:
- Backfills `standard_ggr = handle - payouts` where both exist
- Saves fixed CSV back to `data/processed/` and copies to dashboard

## Channel Distinction: Online vs Retail

The validator reports channel breakdown for every state. Current coverage:

**Online + Retail split (26 states):** AZ, CO, CT, DC, IA, IL, IN, KY, LA, MA, MD, ME, MI, MO, NC(online-only), NH, NJ, OH, PA, RI, TN(online-only), VT(online-only), WV, WY(online-only)

**Retail only (4 states):** DE, MS, MT, NE, SD

**Combined only (3 states):** AR, NV, VA

If `has_channel_split: True` in config but data only has 'combined', a warning is raised to flag the gap.

## Interpreting Common Warnings

**ggr_consistency: handle-payouts != gross_revenue**
Normal for states where GGR = Handle - Payouts - Federal Excise Tax (e.g., AZ). The excise tax causes a ~0.25% gap.

**tax_consistency: tax_paid differs from revenue x rate**
Expected for: graduated tax states (IL), revenue-share models (NH, RI, DE), states where promo deductions change the tax base, or when using gross_revenue instead of net_revenue as the check basis.

**negative_handle: N rows with negative handle**
Normal in small quantities: operator wind-downs (FoxBet in MI), COVID shutdowns (RI April 2020), month-end reversals. Suspicious if >5% of data.

**hold_sanity: hold% outside 1-40%**
Low hold is common in slow months or for small operators. Negative hold means payouts exceeded handle (promos, prior-period adjustments). Very high hold (>40%) usually means handle is understated or data is YTD.

## Python API

```python
from pipeline.validate_and_promote import (
    DataValidator, validate_state, validate_and_promote,
    fix_standard_ggr, promote_state
)

# Single state
result = validate_state("CT")
print(result.passed)       # True/False
print(result.errors)       # list of blocking Issues
print(result.warnings)     # list of non-blocking Issues
print(result.channels)     # {'online': 159, 'retail': 83}

# Fix standard_ggr on a DataFrame
df = pd.read_csv("data/processed/CT.csv")
df = fix_standard_ggr(df)

# Promote after validation
promote_state("CT", df, fix=True)
```

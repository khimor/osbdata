# Skill: QA Check (Data Quality Audit)

## When to use
Run as the final QA gate after scraping, sense-checking, and validating state data. Produces a letter grade (A-F) per state across 7 quality dimensions. Use before shipping data to production consumers or the dashboard.

## Quick Start

```bash
# QA check all states
python3 -m pipeline.qa_check

# QA check specific states
python3 -m pipeline.qa_check CO NY PA

# Verbose output (every individual finding)
python3 -m pipeline.qa_check CO --verbose

# JSON output (for programmatic consumption)
python3 -m pipeline.qa_check --json
```

## What It Checks

### 1. Channel Independence
| Check | Level | What it verifies |
|---|---|---|
| `config_mismatch` | WARN | `has_channel_split=True` in config but only `combined` in data |
| `channel_sum` | FAIL | retail + online != combined (>1% diff per period) |
| `channel_magnitude` | WARN | online < 50% of total handle (unusual for mature markets) |
| `data_leakage` | WARN | identical values across channels in >50% of periods |

### 2. Operator Completeness
| Check | Level | What it verifies |
|---|---|---|
| `operators_exist` | FAIL | config says operator breakdown but 0 operator rows |
| `operator_stability` | WARN/FAIL | operator count drops >40% from recent mode |
| `sum_to_total` | WARN | SUM(operators) != TOTAL row (>1% diff in >20% of periods) |
| `parent_company` | WARN | any operator missing parent_company mapping |

### 3. Sport Breakdown
| Check | Level | What it verifies |
|---|---|---|
| `sports_exist` | WARN | config says sport breakdown but no sport_category data |
| `expected_categories` | WARN | expected sport categories from config missing in data |
| `sport_channel_split` | WARN | state has channel split but sports only in `combined` |
| `sport_handle_sum` | WARN | sport handles >110% of total (possible double counting) |

### 4. Financial Integrity
| Check | Level | What it verifies |
|---|---|---|
| `standard_ggr_coverage` | WARN | handle+payouts exist but standard_ggr null in >10% of rows |
| `median_hold` | WARN | median hold% outside 3-20% range |
| `revenue_sign` | WARN | negative gross_revenue with positive handle in >10% of rows |

### 5. Temporal Integrity
| Check | Level | What it verifies |
|---|---|---|
| `future_dates` | FAIL | any period_end after today |
| `pre_launch` | WARN | any period_end before state launch date |
| `date_gaps` | WARN | missing months in monthly reporting sequence |
| `stale_data` | WARN | same handle value 4+ consecutive months |

### 6. Magnitude (Tier-Aware)
| Check | Level | What it verifies |
|---|---|---|
| `handle_magnitude` | FAIL | median monthly handle above tier max (unit error) |
| `handle_magnitude` | WARN | median monthly handle below tier min |

Tier ranges (monthly handle):
- **Tier 1** (NY, IL, PA, NJ, OH, MI): $300M - $3B
- **Tier 2** (AZ, CO, IN, MA, MD, VA): $50M - $1B
- **Tier 3** (CT, IA, KS, KY, LA, NC, TN): $10M - $500M
- **Tier 4** (ME, MO, NH, RI, WV, WY): $1M - $200M
- **Tier 5** (AR, DC, DE, MS, MT, NE, NV, OR, SD, VT): $100K - $500M

### 7. Completeness
| Check | Level | What it verifies |
|---|---|---|
| `field_coverage` | WARN | expected money field <50% populated |
| `completeness_grade` | INFO | data richness grade based on money column coverage |

## How to Read Results

### Score Interpretation
| Grade | Meaning |
|---|---|
| **A** | 0 fails, 0 warns — production-ready |
| **B** | 0 fails, 1-2 warns — minor gaps, usable |
| **C** | 0 fails, 3-5 warns — notable gaps, investigate |
| **D** | 1 fail OR 6+ warns — data quality issues |
| **F** | 2+ fails — data cannot be trusted |

### Output Icons
- `✓` all checks passed in category
- `⚠` has warnings
- `✗` has failures
- `─` all checks skipped (not applicable)

## Python API

```python
from pipeline.qa_check import QAChecker, qa_check_state, qa_check_all

# Single state
result = qa_check_state("CO")
print(result.score)        # 'C'
print(result.passed)       # True (no FAILs)
print(result.fails)        # []
print(result.warns)        # [QAFinding(...), ...]
print(result.summary_stats) # {'channels': {'pass': 3, 'warn': 0, ...}, ...}

# All states
results = qa_check_all()
for r in results:
    print(f"{r.state_code}: {r.score}")

# Specific states
results = qa_check_all(["NY", "PA", "CO"])

# JSON serialization
from pipeline.qa_check import result_to_json
import json
print(json.dumps(result_to_json(result), indent=2))
```

## What It Does NOT Check

These are handled by the other two pipeline modules (run them first):

| Check | Module |
|---|---|
| Schema validation, type checks | `validate_and_promote` |
| Column swaps, unit errors | `sense_check` |
| YTD-as-monthly detection | `sense_check` |
| GGR formula (handle - payouts = GGR) | `validate_and_promote` |
| Net revenue formula (GGR - promos = net) | `validate_and_promote` |
| Tax rate consistency | `validate_and_promote` |

## Relationship to Other Skills

```
Scraper runs  →  sense_check (catches scraper bugs)
              →  validate_and_promote (schema + consistency)
              →  qa_check (comprehensive quality audit)   ← THIS SKILL
              →  production / dashboard
```

Run sense_check and validate_and_promote FIRST. Those catch structural bugs and schema violations. QA check evaluates data richness, dimensional completeness, and cross-cutting consistency — the checks that matter for institutional consumers.

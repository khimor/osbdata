/**
 * State reporting metadata — what each state reports and why metrics are missing.
 * Source of truth: scrapers/config.py STATE_REGISTRY + scraper docstrings + CSV data audit.
 *
 * Per-metric: { reported: true | false | 'partial', reason?: string }
 * 'partial' = metric available for some periods but not all.
 */

export const STATE_REPORTING_META = {
  AR: {
    handle: { reported: false, reason: 'AR does not report handle. Estimated from hold rate.' },
    standard_ggr: { reported: true },
    payouts: { reported: false, reason: 'AR does not report payouts.' },
    tax_paid: { reported: true },
    has_operator_breakdown: false,
    operator_note: 'AR reports aggregate totals only.',
  },
  AZ: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  CO: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: false,
    operator_note: 'CO reports aggregate totals with sport breakdown only.',
  },
  CT: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  DC: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  DE: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: false, reason: 'DE Lottery model uses revenue sharing; per-period tax not published separately.' },
    has_operator_breakdown: true,
  },
  IA: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  IL: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  IN: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: false, reason: 'IN Gaming Commission does not publish tax amounts in monthly reports.' },
    has_operator_breakdown: true,
  },
  KS: {
    handle: { reported: true },
    standard_ggr: { reported: false, reason: 'KS does not report payouts, so standard GGR cannot be computed.' },
    payouts: { reported: false, reason: 'KS does not report payouts.' },
    tax_paid: { reported: false, reason: 'KS does not publish tax amounts in monthly reports.' },
    has_operator_breakdown: true,
  },
  KY: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  LA: {
    handle: { reported: 'partial', reason: 'LA reports handle for some periods but not consistently across all reports.' },
    standard_ggr: { reported: true },
    payouts: { reported: false, reason: 'LA does not report payouts.' },
    tax_paid: { reported: 'partial', reason: 'Tax data sparse in source reports.' },
    has_operator_breakdown: false,
    operator_note: 'LA reports aggregate totals with sport breakdown only.',
  },
  MA: {
    handle: { reported: true },
    standard_ggr: { reported: false, reason: 'MA does not report payouts, so standard GGR cannot be computed. Gross revenue (AGR) is reported.' },
    payouts: { reported: false, reason: 'MA Gaming Commission does not report payouts.' },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  MD: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  ME: {
    handle: { reported: true },
    standard_ggr: { reported: false, reason: 'ME does not report payouts, so standard GGR cannot be computed.' },
    payouts: { reported: false, reason: 'ME Gambling Control Unit does not report payouts.' },
    tax_paid: { reported: false, reason: 'ME does not publish tax amounts in monthly reports.' },
    has_operator_breakdown: true,
  },
  MI: {
    handle: { reported: true },
    standard_ggr: { reported: false, reason: 'MI does not report payouts, so standard GGR cannot be computed. Reported GGR (AGR) is available.' },
    payouts: { reported: false, reason: 'MI Gaming Control Board does not report payouts.' },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  MO: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: 'partial', reason: 'MO tax data is sparse in some report periods.' },
    has_operator_breakdown: true,
  },
  MS: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: false, reason: 'MS Gaming Commission does not report payouts.' },
    tax_paid: { reported: false, reason: 'MS does not publish tax amounts in monthly revenue reports.' },
    has_operator_breakdown: false,
    operator_note: 'MS reports by region (Northern, Central, Coastal), not by operator.',
  },
  MT: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: false, reason: 'MT Lottery model uses revenue sharing; tax not reported separately.' },
    has_operator_breakdown: false,
    operator_note: 'MT is a state monopoly via Intralot. No operator breakdown.',
  },
  NC: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: 'partial', reason: 'Tax data sparse in early NC reports.' },
    has_operator_breakdown: false,
    operator_note: 'NC reports aggregate mobile totals only.',
  },
  NE: {
    handle: { reported: false, reason: 'NE does not report handle or payouts. Only GGR and tax are published.' },
    standard_ggr: { reported: true },
    payouts: { reported: false, reason: 'NE does not report payouts.' },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  NH: {
    handle: { reported: true },
    standard_ggr: { reported: false, reason: 'NH does not report payouts, so standard GGR cannot be computed.' },
    payouts: { reported: false, reason: 'NH Lottery does not report payouts.' },
    tax_paid: { reported: true },
    has_operator_breakdown: false,
    operator_note: 'DraftKings is the sole operator in NH.',
  },
  NJ: {
    handle: { reported: false, reason: 'NJ tax returns report GGR only. Aggregate handle sourced from separate press releases.' },
    standard_ggr: { reported: false, reason: 'NJ does not report payouts, so standard GGR cannot be computed per operator.' },
    payouts: { reported: false, reason: 'NJ DGE does not report payouts in tax returns.' },
    tax_paid: { reported: 'partial', reason: 'Tax data available in some report periods.' },
    has_operator_breakdown: true,
  },
  NV: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: 'partial', reason: 'Tax data sparse in NV GRI reports.' },
    has_operator_breakdown: false,
    operator_note: 'NV reports statewide aggregates by sport only.',
  },
  NY: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: false, reason: 'NY does not publish tax paid per operator in weekly reports.' },
    has_operator_breakdown: true,
  },
  OH: {
    handle: { reported: true },
    standard_ggr: { reported: false, reason: 'OH does not report payouts, so standard GGR cannot be computed.' },
    payouts: { reported: false, reason: 'OH Casino Control Commission does not report payouts.' },
    tax_paid: { reported: false, reason: 'OH does not publish tax amounts in monthly reports.' },
    has_operator_breakdown: true,
  },
  OR: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: false, reason: 'OR Lottery model uses revenue sharing; tax not reported separately.' },
    has_operator_breakdown: false,
    operator_note: 'DraftKings is the sole online operator via Oregon Lottery Scoreboard.',
  },
  PA: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: false, reason: 'PA Gaming Control Board does not report payouts in revenue reports.' },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
  RI: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: false, reason: 'RI Lottery model uses revenue sharing (51% state); tax not reported separately.' },
    has_operator_breakdown: true,
  },
  SD: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: false,
    operator_note: 'SD reports Deadwood casino aggregates by sport only.',
  },
  TN: {
    handle: { reported: true },
    standard_ggr: {
      reported: 'partial',
      reason: 'GGR reported Nov 2020 - Jun 2023 only. After Jul 2023 tax change (20% GGR to 1.85% handle), only handle and tax are reported.',
    },
    payouts: {
      reported: 'partial',
      reason: 'Payouts reported Nov 2020 - Jun 2023 only.',
    },
    tax_paid: { reported: true },
    has_operator_breakdown: false,
    operator_note: 'TN reports aggregate online totals only.',
  },
  VA: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: false,
    operator_note: 'VA reports aggregate state-level totals only (mobile + casino retail).',
  },
  VT: {
    handle: { reported: true },
    standard_ggr: { reported: 'partial', reason: 'VT data is sparse; GGR not consistently available.' },
    payouts: { reported: 'partial', reason: 'Payouts not consistently available.' },
    tax_paid: { reported: 'partial', reason: 'Tax data sparse in some periods.' },
    has_operator_breakdown: false,
    operator_note: 'VT reports aggregate online totals only.',
  },
  WV: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
    operator_note: 'WV reports by casino venue, not by sportsbook brand.',
  },
  WY: {
    handle: { reported: true },
    standard_ggr: { reported: true },
    payouts: { reported: true },
    tax_paid: { reported: true },
    has_operator_breakdown: true,
  },
};

/**
 * Get the reporting status for a specific metric in a state.
 * Returns { reported: true } as default if state/metric not found.
 */
export function getMetricStatus(stateCode, metricKey) {
  const meta = STATE_REPORTING_META[stateCode];
  if (!meta) return { reported: true };
  const status = meta[metricKey];
  if (!status || typeof status !== 'object') return { reported: true };
  return status;
}

/**
 * Check if a state has per-operator breakdown.
 */
export function hasOperatorBreakdown(stateCode) {
  const meta = STATE_REPORTING_META[stateCode];
  return meta?.has_operator_breakdown !== false;
}

/**
 * Get operator note for a state (if any).
 */
export function getOperatorNote(stateCode) {
  const meta = STATE_REPORTING_META[stateCode];
  return meta?.operator_note || null;
}

import Papa from 'papaparse';
import { STATE_NAMES } from '../utils/colors';

const STATE_CODES = [
  'AR','AZ','CO','CT','DC','DE','IA','IL','IN','KS','KY','LA',
  'MA','MD','ME','MI','MO','MS','MT','NC','NE','NH','NJ','NV',
  'NY','OH','OR','PA','RI','SD','TN','VA','VT','WV','WY'
];

let _allData = null;
let _loading = null;

function parseCsvText(text) {
  const result = Papa.parse(text, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
  });
  return result.data;
}

function toNum(v) {
  if (v == null || v === '' || v === 'NA' || v === 'None') return null;
  const n = Number(v);
  return isNaN(n) ? null : n;
}

function processRow(row) {
  return {
    ...row,
    handle: toNum(row.handle),
    gross_revenue: toNum(row.gross_revenue),
    standard_ggr: toNum(row.standard_ggr),
    promo_credits: toNum(row.promo_credits),
    net_revenue: toNum(row.net_revenue),
    payouts: toNum(row.payouts),
    tax_paid: toNum(row.tax_paid),
    federal_excise_tax: toNum(row.federal_excise_tax),
    hold_pct: toNum(row.hold_pct),
    days_in_period: toNum(row.days_in_period),
    // Provenance columns — keep as strings (not numeric)
    source_file: row.source_file || null,
    source_sheet: row.source_sheet || null,
    source_row: toNum(row.source_row),
    source_column: row.source_column || null,
    source_page: toNum(row.source_page),
    source_table_index: toNum(row.source_table_index),
    source_url: row.source_url || null,
    source_report_url: row.source_report_url || null,
    source_screenshot: row.source_screenshot || null,
    source_raw_line: row.source_raw_line || null,
    source_context: row.source_context || null,
    scrape_timestamp: row.scrape_timestamp || null,
  };
}

/**
 * Filter data by channel. null = all channels (combined view).
 */
function filterByChannel(data, channel) {
  if (!channel) return data;
  return data.filter(r => r.channel === channel);
}

/**
 * Load all state CSVs and merge into a single array.
 * Cached after first load.
 */
export async function loadAllData() {
  if (_allData) return _allData;
  if (_loading) return _loading;

  _loading = (async () => {
    const all = [];
    const fetches = STATE_CODES.map(async (code) => {
      try {
        const resp = await fetch(`/data/${code}.csv`);
        if (!resp.ok) return;
        const text = await resp.text();
        const rows = parseCsvText(text);
        rows.forEach(r => {
          all.push(processRow(r));
        });
      } catch {
        // skip states that fail to load
      }
    });
    await Promise.all(fetches);
    _allData = all;
    _loading = null;
    return all;
  })();

  return _loading;
}

/**
 * Get all unique months (YYYY-MM) that have data, sorted ascending.
 */
export async function getAvailableMonths() {
  const data = await loadAllData();
  const months = new Set();
  for (const row of data) {
    if (row.period_type === 'monthly' && row.period_end && !row.sport_category) {
      months.add(row.period_end.slice(0, 7));
    }
  }
  return [...months].sort();
}

/**
 * Get column names from the dataset.
 */
export async function getColumns() {
  const data = await loadAllData();
  if (!data.length) return [];
  return Object.keys(data[0]);
}

/**
 * Get all data (the full merged dataset).
 */
export async function getAllData() {
  return loadAllData();
}

/**
 * Get data filtered by state and optional filters.
 */
export async function getStateData(stateCode, filters = {}) {
  const data = await loadAllData();
  let filtered = data.filter(r => r.state_code === stateCode);

  if (filters.operator) {
    filtered = filtered.filter(r => r.operator_standard === filters.operator);
  }
  if (filters.channel) {
    filtered = filtered.filter(r => r.channel === filters.channel);
  }
  if (filters.periodType) {
    filtered = filtered.filter(r => r.period_type === filters.periodType);
  }
  if (filters.startDate) {
    filtered = filtered.filter(r => r.period_end >= filters.startDate);
  }
  if (filters.endDate) {
    filtered = filtered.filter(r => r.period_end <= filters.endDate);
  }
  return filtered;
}

/**
 * Compute national summary: per-state aggregates for the most recent complete month.
 */
export async function getNationalSummary(targetMonth = null, channel = null) {
  const data = filterByChannel(await loadAllData(), channel);

  // Find all monthly rows, grouped by state
  const stateMap = {};
  for (const row of data) {
    const sc = row.state_code;
    if (!stateMap[sc]) stateMap[sc] = [];
    stateMap[sc].push(row);
  }

  const states = [];
  for (const [sc, rows] of Object.entries(stateMap)) {
    // Get monthly rows only, exclude sport breakdown rows (but keep sport rows if that's all the state has)
    const monthlyNonSport = rows.filter(r => r.period_type === 'monthly' && !r.sport_category);
    const monthly = monthlyNonSport.length > 0
      ? monthlyNonSport
      : rows.filter(r => r.period_type === 'monthly');
    const useRows = monthly.length > 0 ? monthly : rows.filter(r => !r.sport_category);

    // Find all unique periods
    const periods = [...new Set(useRows.map(r => r.period_end))].sort();

    let latestPeriod;
    if (targetMonth) {
      const matching = periods.filter(p => p.startsWith(targetMonth));
      latestPeriod = matching.length > 0 ? matching[matching.length - 1] : null;
      if (!latestPeriod) continue;
    } else {
      latestPeriod = periods[periods.length - 1];
    }

    const periodIdx = periods.indexOf(latestPeriod);
    const prevPeriod = periodIdx > 0 ? periods[periodIdx - 1] : null;

    // Find YoY period (same month, prior year)
    let yoyPeriod = null;
    if (latestPeriod) {
      const d = new Date(latestPeriod + 'T00:00:00');
      const yoyDate = new Date(d.getFullYear() - 1, d.getMonth(), d.getDate());
      const yoyStr = yoyDate.toISOString().slice(0, 10);
      const closest = periods.filter(p => p.startsWith(yoyStr.slice(0, 7)));
      if (closest.length) yoyPeriod = closest[closest.length - 1];
    }

    // Aggregate latest period
    const latestRows = useRows.filter(r => r.period_end === latestPeriod);

    // Check if there are operator-level rows (not ALL/TOTAL)
    const hasOperators = latestRows.some(r =>
      r.operator_standard && !['TOTAL', 'ALL'].includes(r.operator_standard)
    );

    // Separate TOTAL and operator rows
    const totalRows = latestRows.filter(r => ['TOTAL', 'ALL'].includes(r.operator_standard || ''));
    const opRows = latestRows.filter(r => !['TOTAL', 'ALL'].includes(r.operator_standard || ''));
    const aggRows = hasOperators ? opRows : latestRows;

    let totalHandle = aggRows.reduce((s, r) => s + (r.handle || 0), 0);
    const totalGgr = aggRows.reduce((s, r) => s + (r.standard_ggr ?? r.gross_revenue ?? 0), 0);
    const totalTax = aggRows.reduce((s, r) => s + (r.tax_paid || 0), 0);

    // For states where operators have no handle but TOTAL does (e.g., NJ)
    if (totalHandle === 0 && totalRows.length > 0) {
      totalHandle = totalRows.reduce((s, r) => s + (r.handle || 0), 0);
    }

    const holdPct = totalHandle > 0 ? totalGgr / totalHandle : null;

    // Count unique operators (excluding TOTAL/ALL)
    const operators = new Set(
      useRows.filter(r => r.operator_standard && !['TOTAL','ALL','UNKNOWN'].includes(r.operator_standard))
        .map(r => r.operator_standard)
    );

    function getMetricsForPeriod(period) {
      if (!period) return { handle: null, ggr: null, tax: null };
      const pRows = useRows.filter(r => r.period_end === period);
      const pHasOps = pRows.some(r => r.operator_standard && !['TOTAL','ALL'].includes(r.operator_standard));
      const pOps = pRows.filter(r => !['TOTAL','ALL'].includes(r.operator_standard || ''));
      const pTotals = pRows.filter(r => ['TOTAL','ALL'].includes(r.operator_standard || ''));
      const aggRows = pHasOps ? pOps : pRows;
      let h = aggRows.reduce((s, r) => s + (r.handle || 0), 0);
      if (h === 0 && pTotals.length > 0) {
        h = pTotals.reduce((s, r) => s + (r.handle || 0), 0);
      }
      const g = aggRows.reduce((s, r) => s + (r.standard_ggr ?? r.gross_revenue ?? 0), 0);
      const t = aggRows.reduce((s, r) => s + (r.tax_paid || 0), 0);
      return { handle: h || null, ggr: g || null, tax: t || null };
    }

    const prevMetrics = getMetricsForPeriod(prevPeriod);
    const yoyMetrics = getMetricsForPeriod(yoyPeriod);

    states.push({
      state_code: sc,
      state_name: STATE_NAMES[sc] || sc,
      total_handle: totalHandle,
      total_ggr: totalGgr,
      total_tax: totalTax,
      hold_pct: holdPct,
      latest_period: latestPeriod,
      row_count: rows.length,
      num_operators: operators.size,
      prev_handle: prevMetrics.handle,
      yoy_handle: yoyMetrics.handle,
      yoy_ggr: yoyMetrics.ggr,
      yoy_tax: yoyMetrics.tax,
    });
  }

  return states.sort((a, b) => (b.total_handle || 0) - (a.total_handle || 0));
}

/**
 * Get national monthly time series (sum across all states, monthly only).
 * Returns [{period_end, handle, standard_ggr, ...}]
 */
export async function getNationalTimeSeries() {
  const data = await loadAllData();
  const monthly = data.filter(r => r.period_type === 'monthly' && !r.sport_category);

  // Group by period_end, sum across states
  // Track TOTAL handle per state for NJ-style states
  const byPeriod = {};
  const totalHandleByPeriodState = {};

  for (const row of monthly) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe, handle: 0, standard_ggr: 0, tax_paid: 0 };

    if (['TOTAL', 'ALL'].includes(row.operator_standard)) {
      if (row.handle) {
        const key = `${pe}:${row.state_code}`;
        totalHandleByPeriodState[key] = (totalHandleByPeriodState[key] || 0) + row.handle;
      }
      continue;
    }
    byPeriod[pe].handle += row.handle || 0;
    byPeriod[pe].standard_ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
    byPeriod[pe].tax_paid += row.tax_paid || 0;
  }

  // Add TOTAL handle for states where operators have no handle (NJ)
  for (const [key, totalHandle] of Object.entries(totalHandleByPeriodState)) {
    const [pe, sc] = key.split(':');
    // Check if this state's operators already contributed handle
    const stateOpsHaveHandle = monthly.some(r =>
      r.period_end === pe && r.state_code === sc &&
      !['TOTAL','ALL'].includes(r.operator_standard) && r.handle > 0
    );
    if (!stateOpsHaveHandle && byPeriod[pe]) {
      byPeriod[pe].handle += totalHandle;
    }
  }

  return Object.values(byPeriod).sort((a, b) => a.period_end.localeCompare(b.period_end));
}

/**
 * Get handle by state over time (for stacked area chart).
 * Returns [{period_end, NY: handle, NJ: handle, ...}]
 */
export async function getHandleByStateTimeSeries(topN = 5, channel = null) {
  const data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    !['TOTAL','ALL'].includes(r.operator_standard || '')
  );

  // Find top N states by total handle
  const stateHandles = {};
  for (const row of monthly) {
    stateHandles[row.state_code] = (stateHandles[row.state_code] || 0) + (row.handle || 0);
  }
  const topStates = Object.entries(stateHandles)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([s]) => s);

  // Group by period
  const byPeriod = {};
  for (const row of monthly) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe };
    const key = topStates.includes(row.state_code) ? row.state_code : 'Other';
    byPeriod[pe][key] = (byPeriod[pe][key] || 0) + (row.handle || 0);
  }

  const series = Object.values(byPeriod).sort((a, b) => a.period_end.localeCompare(b.period_end));
  return { series, keys: [...topStates, 'Other'] };
}

/**
 * Get operator summary (national level), optionally filtered by states and channel.
 */
export async function getOperatorSummary(selectedStates = null, channel = null) {
  let data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard) &&
    (!selectedStates || selectedStates.includes(r.state_code))
  );

  const opMap = {};
  for (const row of monthly) {
    const op = row.operator_standard;
    if (!opMap[op]) opMap[op] = { operator: op, handle: 0, ggr: 0, states: new Set() };
    opMap[op].handle += row.handle || 0;
    opMap[op].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
    opMap[op].states.add(row.state_code);
  }

  const totalHandle = Object.values(opMap).reduce((s, o) => s + o.handle, 0);

  return Object.values(opMap)
    .map(o => ({
      operator: o.operator,
      total_handle: o.handle,
      total_ggr: o.ggr,
      market_share: totalHandle > 0 ? o.handle / totalHandle : 0,
      state_count: o.states.size,
      states: [...o.states].sort(),
    }))
    .sort((a, b) => b.total_handle - a.total_handle);
}

/**
 * Get operator summary for the most recent month, optionally filtered by states and channel.
 * Returns { operators, period }.
 */
/**
 * Get operator summary for a date range, optionally filtered by states/channel.
 * startMonth/endMonth in YYYY-MM format. If both null, returns latest month only.
 */
export async function getOperatorSummaryRange(selectedStates = null, channel = null, startMonth = null, endMonth = null) {
  let data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard) &&
    (!selectedStates || selectedStates.includes(r.state_code))
  );

  // Filter to date range
  let rangeRows = monthly;
  if (startMonth || endMonth) {
    rangeRows = monthly.filter(r => {
      const m = r.period_end.slice(0, 7);
      if (startMonth && m < startMonth) return false;
      if (endMonth && m > endMonth) return false;
      return true;
    });
  }

  if (!rangeRows.length) return { operators: [], period: null, startMonth, endMonth };

  const opMap = {};
  for (const row of rangeRows) {
    const op = row.operator_standard;
    if (!opMap[op]) {
      opMap[op] = {
        operator: op, handle: 0, ggr: 0, states: new Set(),
        state_code: row.state_code,
        period_end: row.period_end,
        operator_standard: row.operator_standard,
        source_file: row.source_file,
        source_url: row.source_url,
        source_report_url: row.source_report_url,
        source_screenshot: row.source_screenshot,
        source_raw_line: row.source_raw_line,
        source_context: row.source_context,
        scrape_timestamp: row.scrape_timestamp,
      };
    }
    opMap[op].handle += row.handle || 0;
    opMap[op].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
    opMap[op].states.add(row.state_code);
  }

  const totalHandle = Object.values(opMap).reduce((s, o) => s + o.handle, 0);
  const totalGgr = Object.values(opMap).reduce((s, o) => s + o.ggr, 0);

  const operators = Object.values(opMap)
    .map(o => ({
      ...o,
      hold_pct: o.handle > 0 ? o.ggr / o.handle : null,
      market_share: totalHandle > 0 ? o.handle / totalHandle : 0,
      ggr_share: totalGgr > 0 ? o.ggr / totalGgr : 0,
      state_count: o.states.size,
      states: [...o.states].sort(),
    }))
    .sort((a, b) => b.ggr - a.ggr);

  const periods = [...new Set(rangeRows.map(r => r.period_end))].sort();
  return { operators, startPeriod: periods[0], endPeriod: periods[periods.length - 1] };
}

export async function getOperatorSummaryLatest(selectedStates = null, channel = null) {
  let data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard) &&
    (!selectedStates || selectedStates.includes(r.state_code))
  );

  // Find the most recent period across all filtered states
  const periods = [...new Set(monthly.map(r => r.period_end))].sort();
  const latestPeriod = periods[periods.length - 1];
  if (!latestPeriod) return { operators: [], period: null };

  // Prior period for MoM
  const prevPeriod = periods.length >= 2 ? periods[periods.length - 2] : null;

  // YoY period (same month, prior year)
  let yoyPeriod = null;
  if (latestPeriod) {
    const d = new Date(latestPeriod + 'T00:00:00');
    const yoyMonth = `${d.getFullYear() - 1}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    const yoyMatch = periods.filter(p => p.startsWith(yoyMonth));
    if (yoyMatch.length) yoyPeriod = yoyMatch[yoyMatch.length - 1];
  }

  const latestRows = monthly.filter(r => r.period_end === latestPeriod);
  const prevRows = prevPeriod ? monthly.filter(r => r.period_end === prevPeriod) : [];
  const yoyRows = yoyPeriod ? monthly.filter(r => r.period_end === yoyPeriod) : [];

  const opMap = {};
  for (const row of latestRows) {
    const op = row.operator_standard;
    if (!opMap[op]) {
      opMap[op] = {
        operator: op, handle: 0, ggr: 0, states: new Set(),
        // Provenance from first row
        state_code: row.state_code,
        period_end: row.period_end,
        operator_standard: row.operator_standard,
        source_file: row.source_file,
        source_url: row.source_url,
        source_report_url: row.source_report_url,
        source_screenshot: row.source_screenshot,
        source_raw_line: row.source_raw_line,
        source_context: row.source_context,
        scrape_timestamp: row.scrape_timestamp,
      };
    }
    opMap[op].handle += row.handle || 0;
    opMap[op].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
    opMap[op].states.add(row.state_code);
  }

  const prevMap = {};
  for (const row of prevRows) {
    const op = row.operator_standard;
    if (!prevMap[op]) prevMap[op] = { handle: 0, ggr: 0 };
    prevMap[op].handle += row.handle || 0;
    prevMap[op].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
  }

  const yoyMap = {};
  for (const row of yoyRows) {
    const op = row.operator_standard;
    if (!yoyMap[op]) yoyMap[op] = { handle: 0, ggr: 0 };
    yoyMap[op].handle += row.handle || 0;
    yoyMap[op].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
  }

  const totalHandle = Object.values(opMap).reduce((s, o) => s + o.handle, 0);

  const operators = Object.values(opMap)
    .map(o => ({
      operator: o.operator,
      handle: o.handle,
      ggr: o.ggr,
      hold_pct: o.handle > 0 ? o.ggr / o.handle : null,
      market_share: totalHandle > 0 ? o.handle / totalHandle : 0,
      state_count: o.states.size,
      states: [...o.states].sort(),
      prev_handle: prevMap[o.operator]?.handle || null,
      prev_ggr: prevMap[o.operator]?.ggr || null,
      yoy_handle: yoyMap[o.operator]?.handle || null,
      yoy_ggr: yoyMap[o.operator]?.ggr || null,
    }))
    .sort((a, b) => b.ggr - a.ggr);

  return { operators, period: latestPeriod };
}

/**
 * Get detailed data for a single operator across all states.
 * Returns { summary, stateBreakdown, timeSeries }.
 */
export async function getOperatorDetail(operatorName, channel = null) {
  let data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    r.operator_standard === operatorName
  );

  if (!monthly.length) return { summary: null, stateBreakdown: [], timeSeries: [] };

  // Time series: aggregate across all states per period
  const byPeriod = {};
  for (const row of monthly) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe, handle: 0, ggr: 0 };
    byPeriod[pe].handle += row.handle || 0;
    byPeriod[pe].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
  }
  const timeSeries = Object.values(byPeriod)
    .sort((a, b) => a.period_end.localeCompare(b.period_end))
    .map(p => ({ ...p, hold_pct: p.handle > 0 ? p.ggr / p.handle : null }));

  // Per-state breakdown (latest period per state)
  const stateMap = {};
  for (const row of monthly) {
    const sc = row.state_code;
    if (!stateMap[sc]) stateMap[sc] = { state_code: sc, periods: {} };
    const pe = row.period_end;
    if (!stateMap[sc].periods[pe]) stateMap[sc].periods[pe] = { handle: 0, ggr: 0 };
    stateMap[sc].periods[pe].handle += row.handle || 0;
    stateMap[sc].periods[pe].ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
  }

  const stateBreakdown = Object.values(stateMap).map(s => {
    const periods = Object.keys(s.periods).sort();
    const latest = s.periods[periods[periods.length - 1]];
    const prev = periods.length >= 2 ? s.periods[periods[periods.length - 2]] : null;
    // YoY
    const latestDate = new Date(periods[periods.length - 1] + 'T00:00:00');
    const yoyMonth = `${latestDate.getFullYear() - 1}-${String(latestDate.getMonth() + 1).padStart(2, '0')}`;
    const yoyKey = periods.find(p => p.startsWith(yoyMonth));
    const yoyData = yoyKey ? s.periods[yoyKey] : null;

    return {
      state_code: s.state_code,
      state_name: STATE_NAMES[s.state_code] || s.state_code,
      handle: latest.handle,
      ggr: latest.ggr,
      hold_pct: latest.handle > 0 ? latest.ggr / latest.handle : null,
      latest_period: periods[periods.length - 1],
      prev_handle: prev?.handle || null,
      yoy_handle: yoyData?.handle || null,
    };
  }).sort((a, b) => b.ggr - a.ggr);

  // Overall summary
  const totalHandle = stateBreakdown.reduce((s, st) => s + st.handle, 0);
  const totalGgr = stateBreakdown.reduce((s, st) => s + st.ggr, 0);
  const summary = {
    operator: operatorName,
    total_handle: totalHandle,
    total_ggr: totalGgr,
    hold_pct: totalHandle > 0 ? totalGgr / totalHandle : null,
    state_count: stateBreakdown.length,
  };

  return { summary, stateBreakdown, timeSeries };
}

/**
 * Get operator handle time series (for stacked area chart), optionally filtered by states/channel.
 */
export async function getOperatorTimeSeries(topN = 6, selectedStates = null, channel = null) {
  let data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard) &&
    (!selectedStates || selectedStates.includes(r.state_code))
  );

  // Top operators by total handle
  const opHandles = {};
  for (const row of monthly) {
    opHandles[row.operator_standard] = (opHandles[row.operator_standard] || 0) + (row.handle || 0);
  }
  const topOps = Object.entries(opHandles)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([op]) => op);

  // Build time series
  const byPeriod = {};
  for (const row of monthly) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe };
    const key = topOps.includes(row.operator_standard) ? row.operator_standard : 'Other';
    byPeriod[pe][key] = (byPeriod[pe][key] || 0) + (row.handle || 0);
  }

  const series = Object.values(byPeriod).sort((a, b) => a.period_end.localeCompare(b.period_end));
  return { series, keys: [...topOps, 'Other'] };
}

/**
 * Get operator GGR time series, optionally filtered by states/channel.
 */
export async function getOperatorGgrTimeSeries(topN = 6, selectedStates = null, channel = null) {
  let data = filterByChannel(await loadAllData(), channel);
  const monthly = data.filter(r =>
    r.period_type === 'monthly' &&
    !r.sport_category &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard) &&
    (!selectedStates || selectedStates.includes(r.state_code))
  );

  const opGgr = {};
  for (const row of monthly) {
    opGgr[row.operator_standard] = (opGgr[row.operator_standard] || 0) + (row.standard_ggr ?? row.gross_revenue ?? 0);
  }
  const topOps = Object.entries(opGgr)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([op]) => op);

  const byPeriod = {};
  for (const row of monthly) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe };
    const key = topOps.includes(row.operator_standard) ? row.operator_standard : 'Other';
    byPeriod[pe][key] = (byPeriod[pe][key] || 0) + (row.standard_ggr ?? row.gross_revenue ?? 0);
  }

  const series = Object.values(byPeriod).sort((a, b) => a.period_end.localeCompare(b.period_end));
  return { series, keys: [...topOps, 'Other'] };
}

/**
 * Get all unique state codes that have operator data.
 */
export async function getStatesWithOperatorData() {
  const data = await loadAllData();
  const states = new Set();
  for (const row of data) {
    if (row.operator_standard && !['TOTAL', 'ALL', 'UNKNOWN'].includes(row.operator_standard)) {
      states.add(row.state_code);
    }
  }
  return [...states].sort();
}

/**
 * Get state-level time series.
 */
export async function getStateTimeSeries(stateCode, periodType = 'monthly', channel = null) {
  const data = filterByChannel(await loadAllData(), channel);
  let filtered = data.filter(r => r.state_code === stateCode);

  if (periodType) {
    const hasPeriodType = filtered.some(r => r.period_type === periodType);
    if (hasPeriodType) {
      filtered = filtered.filter(r => r.period_type === periodType);
    }
  }

  // Exclude sport breakdown rows (but keep them if that's all the state has)
  const nonSport = filtered.filter(r => !r.sport_category);
  if (nonSport.length > 0) {
    filtered = nonSport;
  }

  // Aggregate by period — handle channel dedup and TOTAL rows
  const byPeriod = {};
  for (const row of filtered) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = {
      period_end: pe, handle: 0, standard_ggr: 0, tax_paid: 0, count: 0,
      _hasOps: false,
      _hasTotalHandle: false, _totalHandle: 0,
      _hasTotalGgr: false, _totalGgr: 0,
      _hasTotalTax: false, _totalTax: 0,
    };

    const isTotalRow = ['TOTAL', 'ALL'].includes(row.operator_standard);

    // Track TOTAL row metrics separately (for aggregate-only states like VA, NV, TN)
    if (isTotalRow) {
      if (row.handle) {
        byPeriod[pe]._hasTotalHandle = true;
        byPeriod[pe]._totalHandle += row.handle;
      }
      const ggr = row.standard_ggr ?? row.gross_revenue ?? 0;
      if (ggr) {
        byPeriod[pe]._hasTotalGgr = true;
        byPeriod[pe]._totalGgr += ggr;
      }
      if (row.tax_paid) {
        byPeriod[pe]._hasTotalTax = true;
        byPeriod[pe]._totalTax += row.tax_paid;
      }
      continue;
    }

    byPeriod[pe]._hasOps = true;
    byPeriod[pe].handle += row.handle || 0;
    byPeriod[pe].standard_ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
    byPeriod[pe].tax_paid += row.tax_paid || 0;
    byPeriod[pe].count++;
  }

  // For states where operators have no data but TOTAL does (VA, NV, TN, etc.)
  for (const p of Object.values(byPeriod)) {
    if (p.handle === 0 && p._hasTotalHandle) {
      p.handle = p._totalHandle;
    }
    if (p.standard_ggr === 0 && p._hasTotalGgr) {
      p.standard_ggr = p._totalGgr;
    }
    if (p.tax_paid === 0 && p._hasTotalTax) {
      p.tax_paid = p._totalTax;
    }
  }

  return Object.values(byPeriod)
    .map(p => ({ ...p, hold_pct: p.handle > 0 ? p.standard_ggr / p.handle : null }))
    .sort((a, b) => a.period_end.localeCompare(b.period_end));
}

/**
 * Get operator breakdown for a state over time.
 */
export async function getStateOperatorTimeSeries(stateCode, topN = 5, channel = null) {
  const data = filterByChannel(await loadAllData(), channel);
  const filtered = data.filter(r =>
    r.state_code === stateCode &&
    r.period_type === 'monthly' &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard)
  );

  // Determine top operators
  const opHandles = {};
  for (const row of filtered) {
    opHandles[row.operator_standard] = (opHandles[row.operator_standard] || 0) + (row.handle || 0);
  }
  const topOps = Object.entries(opHandles)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([op]) => op);

  // Build time series
  const byPeriod = {};
  for (const row of filtered) {
    const pe = row.period_end;
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe };
    const key = topOps.includes(row.operator_standard) ? row.operator_standard : 'Other';
    byPeriod[pe][key] = (byPeriod[pe][key] || 0) + (row.handle || 0);
  }

  const series = Object.values(byPeriod).sort((a, b) => a.period_end.localeCompare(b.period_end));
  return { series, keys: [...topOps, 'Other'] };
}

/**
 * Get sports handle breakdown for a state from the main CSV (sport_category rows).
 * Returns { series: [{period_end, football: handle, basketball: handle, ...}], keys: [...] }
 * or null if no sports data exists.
 */
export async function getStateSportsTimeSeries(stateCode, channel = null) {
  const data = filterByChannel(await loadAllData(), channel);
  const sportRows = data.filter(r =>
    r.state_code === stateCode &&
    r.period_type === 'monthly' &&
    r.sport_category
  );

  if (!sportRows.length) return null;

  // Avoid channel double counting for sport rows
  const channels = new Set(sportRows.map(r => r.channel));
  let filtered = sportRows;
  if (channels.has('combined') && (channels.has('online') || channels.has('retail'))) {
    filtered = sportRows.filter(r => r.channel !== 'combined');
  }

  const sportSet = new Set();
  const byPeriod = {};
  for (const row of filtered) {
    const pe = row.period_end;
    const sport = row.sport_category;
    const handle = row.handle || 0;
    sportSet.add(sport);
    if (!byPeriod[pe]) byPeriod[pe] = { period_end: pe };
    byPeriod[pe][sport] = (byPeriod[pe][sport] || 0) + handle;
  }

  const keys = [...sportSet].sort();
  const series = Object.values(byPeriod).sort((a, b) => a.period_end.localeCompare(b.period_end));
  return { series, keys };
}

/**
 * Get operator table for a specific period (or most recent) in a state.
 * Returns { operators, period, availablePeriods }.
 */
export async function getStateOperatorTable(stateCode, targetPeriod = null, channel = null) {
  const data = filterByChannel(await loadAllData(), channel);
  let stateRows = data.filter(r =>
    r.state_code === stateCode &&
    r.operator_standard &&
    !['TOTAL', 'ALL', 'UNKNOWN'].includes(r.operator_standard)
  );

  // For aggregate-only states (VA, TN, NV, etc.), fall back to TOTAL rows
  if (stateRows.length === 0) {
    stateRows = data.filter(r =>
      r.state_code === stateCode &&
      r.operator_standard &&
      !r.sport_category
    );
  }

  const monthly = stateRows.filter(r => r.period_type === 'monthly');
  const useRows = monthly.length > 0 ? monthly : stateRows;

  const periods = [...new Set(useRows.map(r => r.period_end))].sort();
  const latestPeriod = (targetPeriod && periods.includes(targetPeriod))
    ? targetPeriod
    : periods[periods.length - 1];

  const periodIdx = periods.indexOf(latestPeriod);
  const prevPeriod = periodIdx > 0 ? periods[periodIdx - 1] : null;

  const latest = useRows.filter(r => r.period_end === latestPeriod);
  const prev = prevPeriod ? useRows.filter(r => r.period_end === prevPeriod) : [];

  const opMap = {};
  for (const row of latest) {
    const op = row.operator_standard;
    if (!opMap[op]) {
      opMap[op] = {
        operator: op, handle: 0, payouts: 0, standard_ggr: 0, gross_revenue: 0,
        promo_credits: 0, tax_paid: 0, net_revenue: 0,
        // Carry forward provenance from first row for this operator
        state_code: row.state_code,
        period_end: row.period_end,
        period_type: row.period_type,
        operator_standard: row.operator_standard,
        source_file: row.source_file,
        source_sheet: row.source_sheet,
        source_row: row.source_row,
        source_column: row.source_column,
        source_page: row.source_page,
        source_url: row.source_url,
        source_report_url: row.source_report_url,
        source_screenshot: row.source_screenshot,
        source_raw_line: row.source_raw_line,
        source_context: row.source_context,
        scrape_timestamp: row.scrape_timestamp,
      };
    }
    opMap[op].handle += row.handle || 0;
    opMap[op].payouts += row.payouts || 0;
    opMap[op].standard_ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
    opMap[op].gross_revenue += row.gross_revenue || 0;
    opMap[op].promo_credits += row.promo_credits || 0;
    opMap[op].tax_paid += row.tax_paid || 0;
    opMap[op].net_revenue += row.net_revenue || 0;
  }

  const totalHandle = Object.values(opMap).reduce((s, o) => s + o.handle, 0);

  const prevMap = {};
  for (const row of prev) {
    const op = row.operator_standard;
    if (!prevMap[op]) prevMap[op] = { handle: 0 };
    prevMap[op].handle += row.handle || 0;
  }

  const operators = Object.values(opMap)
    .map(o => ({
      ...o,
      hold_pct: o.handle > 0 ? o.standard_ggr / o.handle : null,
      market_share: totalHandle > 0 ? o.handle / totalHandle : 0,
      prev_handle: prevMap[o.operator]?.handle || null,
    }))
    .sort((a, b) => b.handle - a.handle);

  return { operators, period: latestPeriod, availablePeriods: periods };
}

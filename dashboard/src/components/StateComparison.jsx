import { useState, useMemo, useEffect } from 'react';
import {
  LineChart, Line,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { useData } from '../hooks/useData';
import { getStateTimeSeries, getNationalSummary } from '../data/loader';
import { formatCurrency, formatPct, formatChange, formatAxisMonth } from '../utils/format';
import { getStateColor, STATE_NAMES } from '../utils/colors';
import ChartCard from './ChartCard';
import ExportButton from './ExportButton';
import SourceableValue from './SourceableValue';
import { PageSkeleton } from './LoadingSkeleton';

const AXIS_TICK = { fill: '#55556a', fontSize: 11, fontFamily: 'JetBrains Mono' };
const GRID_STYLE = { stroke: '#1a1a28', strokeDasharray: 'none' };

const CHANNEL_OPTIONS = [
  { value: null, label: 'Combined' },
  { value: 'online', label: 'Online' },
  { value: 'retail', label: 'In-Person' },
];

const DATE_PRESETS = [
  { label: '6M', months: 6 },
  { label: '1Y', months: 12 },
  { label: '2Y', months: 24 },
  { label: 'ALL', months: null },
];

const METRICS = [
  { key: 'handle', label: 'Handle', format: formatCurrency },
  { key: 'standard_ggr', label: 'GGR', format: formatCurrency },
  { key: 'hold_pct', label: 'Hold %', format: formatPct },
  { key: 'tax_paid', label: 'Tax', format: formatCurrency },
];

function ChartTooltip({ active, payload, label, formatter }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="tooltip-date">{label}</div>
      {payload.filter(p => p.value != null).map((p, i) => (
        <div key={i} className="tooltip-row">
          <span className="tooltip-label" style={{ color: p.color }}>{p.name}</span>
          <span className="tooltip-value">{formatter(p.value)}</span>
        </div>
      ))}
    </div>
  );
}

function filterByRange(data, rangeMonths) {
  if (!data || !rangeMonths) return data;
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - rangeMonths);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  return data.filter(d => d.period_end >= cutoffStr);
}

export default function StateComparison() {
  const [selectedStates, setSelectedStates] = useState([]);
  const [channel, setChannel] = useState(null);
  const [rangeMonths, setRangeMonths] = useState(12);
  const [metric, setMetric] = useState('handle');
  const [stateSeriesMap, setStateSeriesMap] = useState({});
  const [loadingStates, setLoadingStates] = useState(false);

  const { data: allStatesData } = useData(() => getNationalSummary(null, channel), [channel]);

  // Pre-select top 5 states on first load
  useEffect(() => {
    if (allStatesData && selectedStates.length === 0) {
      const top5 = allStatesData.slice(0, 5).map(s => s.state_code);
      setSelectedStates(top5);
    }
  }, [allStatesData]);

  // Fetch time series for each selected state
  useEffect(() => {
    if (!selectedStates.length) {
      setStateSeriesMap({});
      return;
    }

    let cancelled = false;
    setLoadingStates(true);

    Promise.all(
      selectedStates.map(async (sc) => {
        const ts = await getStateTimeSeries(sc, 'monthly', channel);
        return [sc, ts];
      })
    ).then(results => {
      if (cancelled) return;
      const map = {};
      for (const [sc, ts] of results) {
        map[sc] = ts;
      }
      setStateSeriesMap(map);
      setLoadingStates(false);
    });

    return () => { cancelled = true; };
  }, [selectedStates, channel]);

  const metricConfig = METRICS.find(m => m.key === metric);

  // Build merged chart data
  const chartData = useMemo(() => {
    if (!selectedStates.length) return [];
    const allDates = new Set();
    for (const sc of selectedStates) {
      const ts = stateSeriesMap[sc];
      if (ts) ts.forEach(d => allDates.add(d.period_end));
    }
    const sorted = [...allDates].sort();
    const filtered = filterByRange(
      sorted.map(pe => ({ period_end: pe })),
      rangeMonths
    ) || sorted.map(pe => ({ period_end: pe }));

    return filtered.map(({ period_end }) => {
      const row = { period_end, date: formatAxisMonth(period_end) };
      for (const sc of selectedStates) {
        const ts = stateSeriesMap[sc];
        const match = ts?.find(d => d.period_end === period_end);
        row[sc] = match ? match[metric] : null;
      }
      return row;
    });
  }, [stateSeriesMap, selectedStates, metric, rangeMonths]);

  // Build GGR chart data (always shows GGR regardless of metric toggle)
  const ggrChartData = useMemo(() => {
    if (!selectedStates.length) return [];
    const allDates = new Set();
    for (const sc of selectedStates) {
      const ts = stateSeriesMap[sc];
      if (ts) ts.forEach(d => allDates.add(d.period_end));
    }
    const sorted = [...allDates].sort();
    const filtered = filterByRange(
      sorted.map(pe => ({ period_end: pe })),
      rangeMonths
    ) || sorted.map(pe => ({ period_end: pe }));

    return filtered.map(({ period_end }) => {
      const row = { period_end, date: formatAxisMonth(period_end) };
      for (const sc of selectedStates) {
        const ts = stateSeriesMap[sc];
        const match = ts?.find(d => d.period_end === period_end);
        row[sc] = match ? match.standard_ggr : null;
      }
      return row;
    });
  }, [stateSeriesMap, selectedStates, rangeMonths]);

  // Comparison table data - merge national summary with provenance from time series
  const tableData = useMemo(() => {
    if (!allStatesData) return [];
    return allStatesData
      .filter(s => selectedStates.includes(s.state_code))
      .map(st => {
        // Find the latest time series row for provenance
        const ts = stateSeriesMap[st.state_code];
        const latestTs = ts?.[ts.length - 1];
        return { ...st, _ts: latestTs };
      });
  }, [allStatesData, selectedStates, stateSeriesMap]);

  const toggleState = (code) => {
    setSelectedStates(prev =>
      prev.includes(code) ? prev.filter(s => s !== code) : [...prev, code]
    );
  };

  const selectTop = (n) => {
    if (!allStatesData) return;
    setSelectedStates(allStatesData.slice(0, n).map(s => s.state_code));
  };

  const allStateCodes = useMemo(() => {
    if (!allStatesData) return [];
    return allStatesData.map(s => s.state_code);
  }, [allStatesData]);

  const isHoldMetric = metric === 'hold_pct';
  const yFormatter = metricConfig?.format || formatCurrency;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h2 className="page-title">Compare States</h2>
          <div className="page-subtitle">
            {selectedStates.length} states selected - {metricConfig?.label}
          </div>
        </div>
        <div className="page-header-controls">
          <div className="view-toggle" style={{ marginBottom: 0 }}>
            {CHANNEL_OPTIONS.map(opt => (
              <button
                key={opt.label}
                className={`view-toggle-btn ${channel === opt.value ? 'active' : ''}`}
                onClick={() => setChannel(opt.value)}
              >
                {opt.label}
              </button>
            ))}
          </div>
          <div className="date-range-selector">
            {DATE_PRESETS.map(p => (
              <button
                key={p.label}
                className={`date-preset ${rangeMonths === p.months ? 'active' : ''}`}
                onClick={() => setRangeMonths(p.months)}
              >
                {p.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* State picker */}
      <div className="filter-bar" style={{ marginBottom: 'var(--space-4)' }}>
        <div style={{ display: 'flex', gap: 'var(--space-2)', alignItems: 'center' }}>
          <button className="state-filter-action" onClick={() => selectTop(5)}>Top 5</button>
          <button className="state-filter-action" onClick={() => selectTop(10)}>Top 10</button>
          <button className="state-filter-action" onClick={() => setSelectedStates([])}>Clear</button>
        </div>
        <div className="divider" />
        <div className="state-picker-grid">
          {allStateCodes.map(code => (
            <label key={code} className={`state-picker-chip ${selectedStates.includes(code) ? 'active' : ''}`}>
              <input
                type="checkbox"
                checked={selectedStates.includes(code)}
                onChange={() => toggleState(code)}
              />
              <span className="state-picker-dot" style={{ background: getStateColor(code) }} />
              {code}
            </label>
          ))}
        </div>
      </div>

      {/* Metric toggle */}
      <div className="view-toggle" style={{ marginBottom: 'var(--space-6)' }}>
        {METRICS.map(m => (
          <button
            key={m.key}
            className={`view-toggle-btn ${metric === m.key ? 'active' : ''}`}
            onClick={() => setMetric(m.key)}
          >
            {m.label}
          </button>
        ))}
      </div>

      {loadingStates && <PageSkeleton />}

      {!loadingStates && selectedStates.length > 0 && (
        <>
          <div className="charts-row">
            <ChartCard title={`${metricConfig?.label} - Absolute`}>
              <ResponsiveContainer width="100%" height={360}>
                <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis
                    tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false}
                    tickFormatter={v => yFormatter(v)}
                    width={isHoldMetric ? 55 : 70}
                    domain={isHoldMetric ? ['auto', 'auto'] : undefined}
                  />
                  <Tooltip content={<ChartTooltip formatter={yFormatter} />} />
                  <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                  {selectedStates.map(sc => (
                    <Line
                      key={sc} type="monotone" dataKey={sc}
                      stroke={getStateColor(sc)} strokeWidth={2}
                      dot={false} name={STATE_NAMES[sc] || sc}
                      isAnimationActive={false} connectNulls
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard title="Std GGR">
              <ResponsiveContainer width="100%" height={360}>
                <LineChart data={ggrChartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis
                    tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false}
                    tickFormatter={v => formatCurrency(v)}
                    width={70}
                  />
                  <Tooltip content={<ChartTooltip formatter={formatCurrency} />} />
                  <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                  {selectedStates.map(sc => (
                    <Line
                      key={sc} type="monotone" dataKey={sc}
                      stroke={getStateColor(sc)} strokeWidth={2}
                      dot={false} name={STATE_NAMES[sc] || sc}
                      isAnimationActive={false} connectNulls
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>
          </div>

          {tableData.length > 0 && (
            <div className="card">
              <div className="card-header">
                <div className="card-title">State Comparison - Latest Period</div>
                <ExportButton data={tableData} filename="state_comparison" />
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left' }}>State</th>
                      <th>Handle</th>
                      <th>Std GGR</th>
                      <th>Hold %</th>
                      <th>Tax Paid</th>
                      <th>YoY Handle</th>
                      <th style={{ textAlign: 'left' }}>Period</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tableData.map(st => {
                      const yoy = formatChange(st.total_handle, st.yoy_handle);
                      // Build a provenance row from the time series data
                      const prov = st._ts || {};
                      const row = {
                        state_code: st.state_code,
                        period_end: st.latest_period,
                        operator_standard: 'TOTAL',
                        source_file: prov.source_file,
                        source_url: prov.source_url,
                        source_report_url: prov.source_report_url,
                        source_screenshot: prov.source_screenshot,
                        source_raw_line: prov.source_raw_line,
                        source_context: prov.source_context,
                        scrape_timestamp: prov.scrape_timestamp,
                      };
                      return (
                        <tr key={st.state_code}>
                          <td style={{ textAlign: 'left' }}>
                            <span className="color-dot" style={{ background: getStateColor(st.state_code) }} />
                            {st.state_code}
                            <span style={{ color: 'var(--text-tertiary)', marginLeft: 8, fontSize: 12, fontFamily: 'var(--font-body)' }}>
                              {st.state_name}
                            </span>
                          </td>
                          <td><SourceableValue value={st.total_handle} formattedValue={formatCurrency(st.total_handle)} row={row} metric="Handle" /></td>
                          <td><SourceableValue value={st.total_ggr} formattedValue={formatCurrency(st.total_ggr)} row={row} metric="Std GGR" /></td>
                          <td>{formatPct(st.hold_pct)}</td>
                          <td><SourceableValue value={st.total_tax} formattedValue={formatCurrency(st.total_tax)} row={row} metric="Tax Paid" /></td>
                          <td>
                            {yoy ? (
                              <span className={yoy.direction === 'up' ? 'cell-positive' : 'cell-negative'}>
                                {yoy.label}
                              </span>
                            ) : '-'}
                          </td>
                          <td style={{ textAlign: 'left', color: 'var(--text-tertiary)', fontSize: 12, fontFamily: 'var(--font-mono)' }}>
                            {st.latest_period}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {!loadingStates && selectedStates.length === 0 && (
        <div className="loading-state">Select states above to compare</div>
      )}
    </div>
  );
}

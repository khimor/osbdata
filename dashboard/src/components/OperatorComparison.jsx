import { useState, useMemo, useEffect } from 'react';
import {
  LineChart, Line,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { useData } from '../hooks/useData';
import {
  getAllOperatorNames, getOperatorComparisonTimeSeries,
  getOperatorSummaryLatest, getStatesWithOperatorData, getOperatorDetail,
} from '../data/loader';
import { formatCurrency, formatPct, formatChange, formatAxisMonth } from '../utils/format';
import { getOperatorColor, getStateColor, STATE_NAMES } from '../utils/colors';
import { ChevronDown, ChevronRight } from 'lucide-react';
import ChartCard from './ChartCard';
import ExportButton from './ExportButton';
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

export default function OperatorComparison() {
  const [selectedOps, setSelectedOps] = useState([]);
  const [channel, setChannel] = useState(null);
  const [rangeMonths, setRangeMonths] = useState(12);
  const [metric, setMetric] = useState('handle');
  const [selectedStates, setSelectedStates] = useState(null);
  const [showStateFilter, setShowStateFilter] = useState(false);
  const [showOpPicker, setShowOpPicker] = useState(false);
  const [expandedOps, setExpandedOps] = useState({});
  const [opDetails, setOpDetails] = useState({});

  const { data: allOperators } = useData(
    () => getAllOperatorNames(channel, selectedStates), [channel, selectedStates]
  );
  const { data: allStates } = useData(() => getStatesWithOperatorData(), []);
  const { data: latestData, loading: loadingLatest } = useData(
    () => getOperatorSummaryLatest(selectedStates, channel), [selectedStates, channel]
  );

  // Pre-select top 5 on first load
  useEffect(() => {
    if (allOperators && selectedOps.length === 0) {
      setSelectedOps(allOperators.slice(0, 5));
    }
  }, [allOperators]);

  // Fetch comparison time series for selected metric
  const { data: metricSeries, loading: loadingMetric } = useData(
    () => selectedOps.length > 0
      ? getOperatorComparisonTimeSeries(selectedOps, metric, channel, selectedStates)
      : Promise.resolve([]),
    [selectedOps, metric, channel, selectedStates]
  );

  // Always fetch GGR for the right chart
  const { data: ggrSeries, loading: loadingGgr } = useData(
    () => selectedOps.length > 0
      ? getOperatorComparisonTimeSeries(selectedOps, 'standard_ggr', channel, selectedStates)
      : Promise.resolve([]),
    [selectedOps, channel, selectedStates]
  );

  const metricConfig = METRICS.find(m => m.key === metric);
  const isHold = metric === 'hold_pct';
  const yFormatter = metricConfig?.format || formatCurrency;

  const metricChartData = useMemo(() => {
    if (!metricSeries) return [];
    const filtered = filterByRange(metricSeries, rangeMonths) || metricSeries;
    return filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) }));
  }, [metricSeries, rangeMonths]);

  const ggrChartData = useMemo(() => {
    if (!ggrSeries) return [];
    const filtered = filterByRange(ggrSeries, rangeMonths) || ggrSeries;
    return filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) }));
  }, [ggrSeries, rangeMonths]);

  // Table: latest data for selected operators
  const tableData = useMemo(() => {
    if (!latestData?.operators) return [];
    return latestData.operators.filter(op => selectedOps.includes(op.operator));
  }, [latestData, selectedOps]);

  const toggleOp = (op) => {
    setSelectedOps(prev =>
      prev.includes(op) ? prev.filter(o => o !== op) : [...prev, op]
    );
  };

  const toggleExpand = async (opName) => {
    if (expandedOps[opName]) {
      setExpandedOps(prev => ({ ...prev, [opName]: false }));
      return;
    }
    // Fetch detail if not cached
    if (!opDetails[opName]) {
      const detail = await getOperatorDetail(opName, channel);
      setOpDetails(prev => ({ ...prev, [opName]: detail.stateBreakdown }));
    }
    setExpandedOps(prev => ({ ...prev, [opName]: true }));
  };

  const selectTop = (n) => {
    if (!allOperators) return;
    setSelectedOps(allOperators.slice(0, n));
  };

  const stateFilterLabel = !selectedStates
    ? 'All States'
    : selectedStates.length === 0
      ? 'No States'
      : selectedStates.length <= 3
        ? selectedStates.join(', ')
        : `${selectedStates.length} States`;

  const loading = loadingMetric || loadingGgr || loadingLatest;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h2 className="page-title">Compare Operators</h2>
          <div className="page-subtitle">
            {selectedOps.length} operators selected - {metricConfig?.label}
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
          <div style={{ position: 'relative' }}>
            <button className="btn" onClick={() => setShowStateFilter(!showStateFilter)}>
              {stateFilterLabel} &#9662;
            </button>
            {showStateFilter && (
              <div className="state-filter-dropdown">
                <div className="state-filter-actions">
                  <button className="state-filter-action" onClick={() => setSelectedStates(null)}>All</button>
                  <button className="state-filter-action" onClick={() => setSelectedStates([])}>None</button>
                </div>
                <div className="state-filter-grid">
                  {(allStates || []).map(code => (
                    <label key={code} className="state-filter-item">
                      <input
                        type="checkbox"
                        checked={!selectedStates || selectedStates.includes(code)}
                        onChange={() => {
                          if (!selectedStates) {
                            setSelectedStates((allStates || []).filter(s => s !== code));
                          } else if (selectedStates.includes(code)) {
                            const next = selectedStates.filter(s => s !== code);
                            setSelectedStates(next.length > 0 ? next : null);
                          } else {
                            setSelectedStates([...selectedStates, code]);
                          }
                        }}
                      />
                      <span>{code}</span>
                      <span className="state-filter-name">{STATE_NAMES[code] || ''}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}
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

      {/* Operator picker - clean dropdown */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', marginBottom: 'var(--space-4)', flexWrap: 'wrap' }}>
        <div style={{ position: 'relative' }}>
          <button className="btn" onClick={() => setShowOpPicker(!showOpPicker)}>
            {selectedOps.length === 0 ? 'Select Operators' : `${selectedOps.length} Operator${selectedOps.length > 1 ? 's' : ''}`} &#9662;
          </button>
          {showOpPicker && (
            <div className="state-filter-dropdown" style={{ minWidth: 280 }}>
              <div className="state-filter-actions">
                <button className="state-filter-action" onClick={() => selectTop(5)}>Top 5</button>
                <button className="state-filter-action" onClick={() => selectTop(10)}>Top 10</button>
                <button className="state-filter-action" onClick={() => setSelectedOps([])}>Clear</button>
              </div>
              <div className="state-filter-grid" style={{ gridTemplateColumns: '1fr' }}>
                {(allOperators || []).map(op => (
                  <label key={op} className="state-filter-item">
                    <input
                      type="checkbox"
                      checked={selectedOps.includes(op)}
                      onChange={() => toggleOp(op)}
                    />
                    <span className="color-dot" style={{ background: getOperatorColor(op) }} />
                    <span>{op}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
        {/* Selected operator tags */}
        {selectedOps.map(op => (
          <span key={op} style={{
            display: 'inline-flex', alignItems: 'center', gap: 4,
            padding: '3px 8px', background: 'var(--bg-active)', borderRadius: 'var(--radius-sm)',
            fontSize: 12, color: 'var(--text-primary)', fontFamily: 'var(--font-body)',
          }}>
            <span className="color-dot" style={{ background: getOperatorColor(op) }} />
            {op}
            <span
              style={{ cursor: 'pointer', marginLeft: 2, color: 'var(--text-tertiary)' }}
              onClick={() => toggleOp(op)}
            >x</span>
          </span>
        ))}
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

      {loading && <PageSkeleton />}

      {!loading && selectedOps.length > 0 && (
        <>
          <div className="charts-row">
            <ChartCard title={metricConfig?.label}>
              <ResponsiveContainer width="100%" height={360}>
                <LineChart data={metricChartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis
                    tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false}
                    tickFormatter={v => yFormatter(v)}
                    width={isHold ? 55 : 70}
                    domain={isHold ? ['auto', 'auto'] : undefined}
                  />
                  <Tooltip content={<ChartTooltip formatter={yFormatter} />} />
                  <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                  {selectedOps.map(op => (
                    <Line
                      key={op} type="monotone" dataKey={op}
                      stroke={getOperatorColor(op)} strokeWidth={2}
                      dot={false} name={op} isAnimationActive={false} connectNulls
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
                  <YAxis tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                  <Tooltip content={<ChartTooltip formatter={formatCurrency} />} />
                  <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                  {selectedOps.map(op => (
                    <Line
                      key={op} type="monotone" dataKey={op}
                      stroke={getOperatorColor(op)} strokeWidth={2}
                      dot={false} name={op} isAnimationActive={false} connectNulls
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>
          </div>

          {tableData.length > 0 && (
            <div className="card">
              <div className="card-header">
                <div className="card-title">Operator Comparison - Latest Month</div>
                <ExportButton data={tableData} filename="operator_comparison" />
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left', width: 40 }}>#</th>
                      <th style={{ textAlign: 'left' }}>Operator</th>
                      <th>Std GGR</th>
                      <th>Handle</th>
                      <th>Hold %</th>
                      <th>Market Share</th>
                      <th>YoY Handle</th>
                      <th>States</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tableData.map((op, i) => {
                      const yoy = formatChange(op.handle, op.yoy_handle);
                      const totalGgr = tableData.reduce((s, o) => s + o.ggr, 0);
                      const ggrShare = totalGgr > 0 ? op.ggr / totalGgr : 0;
                      const isExpanded = expandedOps[op.operator];
                      const stateRows = opDetails[op.operator] || [];
                      return (
                        <>
                          <tr key={op.operator} className="clickable" onClick={() => toggleExpand(op.operator)}>
                            <td style={{ textAlign: 'left', color: 'var(--text-tertiary)' }}>
                              {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                            </td>
                            <td style={{ textAlign: 'left', fontWeight: 500 }}>
                              <span className="color-dot" style={{ background: getOperatorColor(op.operator) }} />
                              {op.operator}
                            </td>
                            <td>{formatCurrency(op.ggr)}</td>
                            <td>{formatCurrency(op.handle)}</td>
                            <td>{formatPct(op.hold_pct)}</td>
                            <td>{formatPct(ggrShare)}</td>
                            <td>
                              {yoy ? (
                                <span className={yoy.direction === 'up' ? 'cell-positive' : 'cell-negative'}>
                                  {yoy.label}
                                </span>
                              ) : '-'}
                            </td>
                            <td style={{ color: 'var(--text-tertiary)', fontSize: 12, fontFamily: 'var(--font-mono)' }}>
                              {op.state_count}
                            </td>
                          </tr>
                          {isExpanded && stateRows.map(st => {
                            const stYoy = formatChange(st.handle, st.yoy_handle);
                            return (
                              <tr key={op.operator + '-' + st.state_code} style={{ background: 'var(--bg-root)' }}>
                                <td></td>
                                <td style={{ textAlign: 'left', paddingLeft: 28, fontSize: 12 }}>
                                  <span className="color-dot" style={{ background: getStateColor(st.state_code) }} />
                                  {st.state_code}
                                  <span style={{ color: 'var(--text-tertiary)', marginLeft: 6, fontFamily: 'var(--font-body)' }}>
                                    {st.state_name}
                                  </span>
                                </td>
                                <td style={{ fontSize: 12 }}>{formatCurrency(st.ggr)}</td>
                                <td style={{ fontSize: 12 }}>{formatCurrency(st.handle)}</td>
                                <td style={{ fontSize: 12 }}>{formatPct(st.hold_pct)}</td>
                                <td style={{ fontSize: 12 }}></td>
                                <td style={{ fontSize: 12 }}>
                                  {stYoy ? (
                                    <span className={stYoy.direction === 'up' ? 'cell-positive' : 'cell-negative'}>
                                      {stYoy.label}
                                    </span>
                                  ) : '-'}
                                </td>
                                <td></td>
                              </tr>
                            );
                          })}
                          {isExpanded && stateRows.length === 0 && (
                            <tr style={{ background: 'var(--bg-root)' }}>
                              <td></td>
                              <td colSpan={7} style={{ textAlign: 'left', paddingLeft: 28, fontSize: 12, color: 'var(--text-tertiary)' }}>
                                Loading state breakdown...
                              </td>
                            </tr>
                          )}
                        </>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {!loading && selectedOps.length === 0 && (
        <div className="loading-state">Select operators above to compare</div>
      )}
    </div>
  );
}

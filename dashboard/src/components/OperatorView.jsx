import { useState, useMemo } from 'react';
import {
  AreaChart, Area,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { useData } from '../hooks/useData';
import {
  getOperatorSummaryLatest, getOperatorSummaryRange, getOperatorTimeSeries,
  getOperatorGgrTimeSeries, getStatesWithOperatorData, getAvailableMonths,
} from '../data/loader';
import { formatCurrency, formatPct, formatChange, formatAxisMonth } from '../utils/format';
import { getOperatorColor, STATE_NAMES } from '../utils/colors';
import ChartCard from './ChartCard';
import ExportButton from './ExportButton';
import SourceableValue from './SourceableValue';

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

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="tooltip-date">{label}</div>
      {payload.filter(p => p.value > 0).map((p, i) => (
        <div key={i} className="tooltip-row">
          <span className="tooltip-label" style={{ color: p.color }}>{p.name}</span>
          <span className="tooltip-value">{formatCurrency(p.value)}</span>
        </div>
      ))}
    </div>
  );
}

function filterByRange(series, rangeMonths) {
  if (!series || !rangeMonths) return series;
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - rangeMonths);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  return series.filter(d => d.period_end >= cutoffStr);
}

export default function OperatorView() {
  const [channel, setChannel] = useState(null);
  const [selectedStates, setSelectedStates] = useState(null);
  const [showStateFilter, setShowStateFilter] = useState(false);
  const [rangeMonths, setRangeMonths] = useState(12);
  const [tableStart, setTableStart] = useState(null);
  const [tableEnd, setTableEnd] = useState(null);

  const { data: allStates } = useData(() => getStatesWithOperatorData(), []);
  const { data: availableMonths } = useData(() => getAvailableMonths(), []);

  const hasTableRange = tableStart || tableEnd;

  const { data: latestData, loading: loadingOps } = useData(
    () => hasTableRange
      ? getOperatorSummaryRange(selectedStates, channel, tableStart, tableEnd)
      : getOperatorSummaryLatest(selectedStates, channel),
    [selectedStates, channel, tableStart, tableEnd]
  );
  const { data: handleTS, loading: loadingHandleTS } = useData(
    () => getOperatorTimeSeries(6, selectedStates, channel),
    [selectedStates, channel]
  );
  const { data: ggrTS, loading: loadingGgrTS } = useData(
    () => getOperatorGgrTimeSeries(6, selectedStates, channel),
    [selectedStates, channel]
  );

  const operators = latestData?.operators;
  const latestPeriod = latestData?.period || latestData?.endPeriod;

  const totals = useMemo(() => {
    if (!operators) return null;
    const totalHandle = operators.reduce((s, o) => s + o.handle, 0);
    const totalGgr = operators.reduce((s, o) => s + o.ggr, 0);
    const avgHold = totalHandle > 0 ? totalGgr / totalHandle : null;
    return { totalHandle, totalGgr, avgHold, opCount: operators.length };
  }, [operators]);

  const handleChartData = useMemo(() => {
    if (!handleTS) return { series: [], keys: [] };
    const filtered = filterByRange(handleTS.series, rangeMonths) || handleTS.series;
    return {
      series: filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) })),
      keys: handleTS.keys,
    };
  }, [handleTS, rangeMonths]);

  const ggrChartData = useMemo(() => {
    if (!ggrTS) return { series: [], keys: [] };
    const filtered = filterByRange(ggrTS.series, rangeMonths) || ggrTS.series;
    return {
      series: filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) })),
      keys: ggrTS.keys,
    };
  }, [ggrTS, rangeMonths]);

  const toggleState = (code) => {
    setSelectedStates(prev =>
      prev.includes(code) ? prev.filter(s => s !== code) : [...prev, code]
    );
  };

  const selectAllStates = () => setSelectedStates(null);
  const clearStates = () => setSelectedStates([]);

  const loading = loadingOps || loadingHandleTS || loadingGgrTS;

  const stateFilterLabel = !selectedStates
    ? 'All States'
    : selectedStates.length === 0
      ? 'No States'
      : selectedStates.length <= 3
        ? selectedStates.join(', ')
      : `${selectedStates.length} States`;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h2 className="page-title">Operator View</h2>
          <div className="page-subtitle">
            {stateFilterLabel}{latestPeriod ? ` - ${formatAxisMonth(latestPeriod)}` : ''}
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
            <button
              className="btn"
              onClick={() => setShowStateFilter(!showStateFilter)}
            >
              {stateFilterLabel} ▾
            </button>
            {showStateFilter && (
              <div className="state-filter-dropdown">
                <div className="state-filter-actions">
                  <button className="state-filter-action" onClick={selectAllStates}>All</button>
                  <button className="state-filter-action" onClick={clearStates}>None</button>
                </div>
                <div className="state-filter-grid">
                  {(allStates || []).map(code => (
                    <label key={code} className="state-filter-item">
                      <input
                        type="checkbox"
                        checked={!selectedStates || selectedStates.includes(code)}
                        onChange={() => {
                          if (!selectedStates) {
                            // Switching from "all" to specific: select all except this one
                            setSelectedStates((allStates || []).filter(s => s !== code));
                          } else {
                            toggleState(code);
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

      {loading && <div className="loading-state">Loading operator data...</div>}

      {totals && (
        <div className="stat-cards">
          <div className="stat-card">
            <span className="stat-label">Total Handle</span>
            <span className="stat-value">{formatCurrency(totals.totalHandle)}</span>
          </div>
          <div className="stat-card">
            <span className="stat-label">Total Std GGR</span>
            <span className="stat-value">{formatCurrency(totals.totalGgr)}</span>
          </div>
          <div className="stat-card">
            <span className="stat-label">Avg Hold %</span>
            <span className="stat-value">{formatPct(totals.avgHold)}</span>
          </div>
          <div className="stat-card">
            <span className="stat-label">Operators</span>
            <span className="stat-value">{totals.opCount}</span>
          </div>
        </div>
      )}

      {!loading && (
        <>
          <div className="charts-row">
            <ChartCard title="Operator Handle Over Time">
              <ResponsiveContainer width="100%" height={320}>
                <AreaChart data={handleChartData.series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                  <Tooltip content={<ChartTooltip />} />
                  <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                  {handleChartData.keys.map(key => (
                    <Area
                      key={key} type="monotone" dataKey={key}
                      stackId="1" stroke={getOperatorColor(key)}
                      fill={getOperatorColor(key)} fillOpacity={0.6}
                      name={key} isAnimationActive={false}
                    />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard title="Operator GGR Over Time">
              <ResponsiveContainer width="100%" height={320}>
                <AreaChart data={ggrChartData.series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                  <Tooltip content={<ChartTooltip />} />
                  <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                  {ggrChartData.keys.map(key => (
                    <Area
                      key={key} type="monotone" dataKey={key}
                      stackId="1" stroke={getOperatorColor(key)}
                      fill={getOperatorColor(key)} fillOpacity={0.6}
                      name={key} isAnimationActive={false}
                    />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            </ChartCard>
          </div>

          {operators?.length > 0 && (
            <div className="card">
              <div className="card-header">
                <div className="card-title">
                  Operator Rankings - {hasTableRange
                    ? `${tableStart ? formatAxisMonth(tableStart + '-01') : 'Start'} to ${tableEnd ? formatAxisMonth(tableEnd + '-01') : 'Latest'}`
                    : latestPeriod ? formatAxisMonth(latestPeriod) : ''}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)' }}>
                  <div className="month-range-picker">
                    <input
                      type="month"
                      value={tableStart || ''}
                      min={availableMonths?.[0] || ''}
                      max={tableEnd || availableMonths?.[availableMonths.length - 1] || ''}
                      onChange={e => setTableStart(e.target.value || null)}
                    />
                    <span className="range-separator">to</span>
                    <input
                      type="month"
                      value={tableEnd || ''}
                      min={tableStart || availableMonths?.[0] || ''}
                      max={availableMonths?.[availableMonths.length - 1] || ''}
                      onChange={e => setTableEnd(e.target.value || null)}
                    />
                  </div>
                  <ExportButton data={operators} filename="operator_rankings" />
                </div>
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left', width: 40 }}>#</th>
                      <th style={{ textAlign: 'left' }}>Operator</th>
                      <th>Std GGR</th>
                      <th>GGR Share</th>
                      <th>Handle</th>
                      <th>Handle Share</th>
                      <th>Hold %</th>
                      <th>MoM Handle</th>
                      <th>YoY Handle</th>
                      <th>States</th>
                    </tr>
                  </thead>
                  <tbody>
                    {operators.map((op, i) => {
                      const mom = !hasTableRange ? formatChange(op.handle, op.prev_handle) : null;
                      const yoy = !hasTableRange ? formatChange(op.handle, op.yoy_handle) : null;
                      const ggrShare = op.ggr_share != null ? op.ggr_share : (() => {
                        const totalGgr = operators.reduce((s, o) => s + o.ggr, 0);
                        return totalGgr > 0 ? op.ggr / totalGgr : 0;
                      })();
                      return (
                        <tr key={op.operator}>
                          <td style={{ textAlign: 'left', color: 'var(--text-tertiary)' }}>{i + 1}</td>
                          <td style={{ textAlign: 'left' }}>
                            <span className="color-dot" style={{ background: getOperatorColor(op.operator) }} />
                            {op.operator}
                          </td>
                          <td><SourceableValue value={op.ggr} formattedValue={formatCurrency(op.ggr)} row={op} metric="Std GGR" /></td>
                          <td>{formatPct(ggrShare)}</td>
                          <td><SourceableValue value={op.handle} formattedValue={formatCurrency(op.handle)} row={op} metric="Handle" /></td>
                          <td>{formatPct(op.market_share)}</td>
                          <td>{formatPct(op.hold_pct)}</td>
                          <td>
                            {mom ? (
                              <span className={mom.direction === 'up' ? 'cell-positive' : 'cell-negative'}>
                                {mom.label}
                              </span>
                            ) : '-'}
                          </td>
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
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

import { useState, useMemo } from 'react';
import {
  AreaChart, Area,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { useData } from '../hooks/useData';
import {
  getNationalSummary, getHandleByStateTimeSeries, getAvailableMonths,
} from '../data/loader';
import { formatCurrency, formatPct, formatChange, formatAxisMonth } from '../utils/format';
import { getStateColor, STATE_NAMES } from '../utils/colors';
import ChartCard from './ChartCard';
import ExportButton from './ExportButton';
import MetricInfo from './MetricInfo';
import { PageSkeleton } from './LoadingSkeleton';

const AXIS_TICK = { fill: '#55556a', fontSize: 11, fontFamily: 'JetBrains Mono' };
const GRID_STYLE = { stroke: '#1a1a28', strokeDasharray: 'none' };
const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function formatMonthLabel(ym) {
  const [y, m] = ym.split('-');
  return `${MONTH_NAMES[parseInt(m, 10) - 1]} ${y}`;
}

function CustomTooltip({ active, payload, label }) {
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

const CHANNEL_OPTIONS = [
  { value: null, label: 'Combined' },
  { value: 'online', label: 'Online' },
  { value: 'retail', label: 'In-Person' },
];

export default function NationalOverview({ onNavigateToState }) {
  const [selectedMonth, setSelectedMonth] = useState(null);
  const [rangeStart, setRangeStart] = useState(null);
  const [rangeEnd, setRangeEnd] = useState(null);
  const [channel, setChannel] = useState(null);

  const { data: availableMonths } = useData(() => getAvailableMonths(), []);
  const { data: states, loading: loadingStates } = useData(() => getNationalSummary(selectedMonth, channel), [selectedMonth, channel]);
  const { data: handleTS, loading: loadingTS } = useData(() => getHandleByStateTimeSeries(6, channel), [channel]);

  const totals = useMemo(() => {
    if (!states) return null;
    const totalHandle = states.reduce((s, st) => s + (st.total_handle || 0), 0);
    const totalGgr = states.reduce((s, st) => s + (st.total_ggr || 0), 0);
    const totalTax = states.reduce((s, st) => s + (st.total_tax || 0), 0);
    const avgHold = totalHandle > 0 ? totalGgr / totalHandle : null;

    // Comparable YoY: only states reporting in both current and prior year
    const compStates = states.filter(st =>
      st.total_handle > 0 && st.yoy_handle > 0
    );

    let yoyHandlePct = null, yoyGgrPct = null, yoyTaxPct = null, yoyHoldBps = null;
    let compCount = compStates.length;

    if (compCount > 0) {
      const curHandle = compStates.reduce((s, st) => s + st.total_handle, 0);
      const curGgr = compStates.reduce((s, st) => s + st.total_ggr, 0);
      const curTax = compStates.reduce((s, st) => s + (st.total_tax || 0), 0);

      const priorHandle = compStates.reduce((s, st) => s + st.yoy_handle, 0);
      const priorGgr = compStates.reduce((s, st) => s + (st.yoy_ggr || 0), 0);
      const priorTax = compStates.reduce((s, st) => s + (st.yoy_tax || 0), 0);

      if (priorHandle > 0) yoyHandlePct = (curHandle - priorHandle) / priorHandle;
      if (priorGgr > 0) yoyGgrPct = (curGgr - priorGgr) / priorGgr;
      if (priorTax > 0) yoyTaxPct = (curTax - priorTax) / priorTax;

      const curHold = curHandle > 0 ? curGgr / curHandle : null;
      const priorHold = priorHandle > 0 ? priorGgr / priorHandle : null;
      if (curHold != null && priorHold != null) {
        yoyHoldBps = Math.round((curHold - priorHold) * 10000);
      }
    }

    return {
      totalHandle, totalGgr, totalTax, avgHold, stateCount: states.length,
      yoyHandlePct, yoyGgrPct, yoyTaxPct, yoyHoldBps, compCount,
    };
  }, [states]);

  const areaData = useMemo(() => {
    if (!handleTS) return { series: [], keys: [] };
    let filtered = handleTS.series;
    if (rangeStart || rangeEnd) {
      filtered = filtered.filter(d => {
        const m = formatAxisMonth(d.period_end);
        if (rangeStart && m < rangeStart) return false;
        if (rangeEnd && m > rangeEnd) return false;
        return true;
      });
    } else {
      filtered = filtered.slice(-24);
    }
    return {
      series: filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) })),
      keys: handleTS.keys,
    };
  }, [handleTS, rangeStart, rangeEnd]);

  const chartTitle = useMemo(() => {
    if (rangeStart && rangeEnd) return `Monthly Handle by State (${formatMonthLabel(rangeStart)} - ${formatMonthLabel(rangeEnd)})`;
    if (rangeStart) return `Monthly Handle by State (from ${formatMonthLabel(rangeStart)})`;
    if (rangeEnd) return `Monthly Handle by State (to ${formatMonthLabel(rangeEnd)})`;
    return 'Monthly Handle by State (Last 24 Mo)';
  }, [rangeStart, rangeEnd]);

  const monthBounds = useMemo(() => {
    if (!availableMonths?.length) return { min: '', max: '' };
    return { min: availableMonths[0], max: availableMonths[availableMonths.length - 1] };
  }, [availableMonths]);


  if (loadingStates || loadingTS) {
    return <PageSkeleton />;
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h2 className="page-title">National Overview</h2>
          <div className="page-subtitle">
            {selectedMonth ? formatMonthLabel(selectedMonth) : 'Most recent reporting period per state'}
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
          {availableMonths?.length > 0 && (
            <select
              value={selectedMonth || ''}
              onChange={e => setSelectedMonth(e.target.value || null)}
            >
              <option value="">Latest Period</option>
              {[...availableMonths].reverse().map(m => (
                <option key={m} value={m}>{formatMonthLabel(m)}</option>
              ))}
            </select>
          )}
        </div>
      </div>

      {totals && (
        <div className="stat-cards">
          <div className="stat-card">
            <span className="stat-label">US Total Handle</span>
            <span className="stat-value">{formatCurrency(totals.totalHandle)}</span>
            {totals.yoyHandlePct != null && (
              <span className={`stat-change ${totals.yoyHandlePct >= 0 ? 'positive' : 'negative'}`}>
                {totals.yoyHandlePct >= 0 ? '+' : ''}{(totals.yoyHandlePct * 100).toFixed(1)}% YoY
              </span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">US Total Std GGR</span>
            <span className="stat-value">{formatCurrency(totals.totalGgr)}</span>
            {totals.yoyGgrPct != null && (
              <span className={`stat-change ${totals.yoyGgrPct >= 0 ? 'positive' : 'negative'}`}>
                {totals.yoyGgrPct >= 0 ? '+' : ''}{(totals.yoyGgrPct * 100).toFixed(1)}% YoY
              </span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Avg Hold %</span>
            <span className="stat-value">{formatPct(totals.avgHold)}</span>
            {totals.yoyHoldBps != null && (
              <span className={`stat-change ${totals.yoyHoldBps >= 0 ? 'positive' : 'negative'}`}>
                {totals.yoyHoldBps >= 0 ? '+' : ''}{totals.yoyHoldBps} bps YoY
              </span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Active States</span>
            <span className="stat-value">{totals.stateCount}</span>
            {totals.compCount > 0 && (
              <span className="stat-change muted">{totals.compCount} comparable</span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Tax Revenue</span>
            <span className="stat-value">{formatCurrency(totals.totalTax)}</span>
            {totals.yoyTaxPct != null && (
              <span className={`stat-change ${totals.yoyTaxPct >= 0 ? 'positive' : 'negative'}`}>
                {totals.yoyTaxPct >= 0 ? '+' : ''}{(totals.yoyTaxPct * 100).toFixed(1)}% YoY
              </span>
            )}
          </div>
        </div>
      )}

      <div className="charts-row single">
        <ChartCard
          title={chartTitle}
          action={
            <div className="month-range-picker">
              <input
                type="month"
                value={rangeStart || ''}
                min={monthBounds.min}
                max={rangeEnd || monthBounds.max}
                onChange={e => setRangeStart(e.target.value || null)}
              />
              <span className="range-separator">to</span>
              <input
                type="month"
                value={rangeEnd || ''}
                min={rangeStart || monthBounds.min}
                max={monthBounds.max}
                onChange={e => setRangeEnd(e.target.value || null)}
              />
            </div>
          }
        >
          <ResponsiveContainer width="100%" height={320}>
            <AreaChart data={areaData.series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid {...GRID_STYLE} vertical={false} />
              <XAxis
                dataKey="date"
                tick={AXIS_TICK}
                axisLine={{ stroke: '#1a1a28' }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={AXIS_TICK}
                axisLine={{ stroke: '#1a1a28' }}
                tickLine={false}
                tickFormatter={v => formatCurrency(v)}
                width={70}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
              {areaData.keys.map(key => (
                <Area
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stackId="1"
                  stroke={getStateColor(key)}
                  fill={getStateColor(key)}
                  fillOpacity={0.6}
                  name={STATE_NAMES[key] || key}
                  isAnimationActive={false}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title">State Rankings - {selectedMonth ? formatMonthLabel(selectedMonth) : (states?.length ? formatMonthLabel(states[0].latest_period?.slice(0, 7)) : '')}</div>
          <ExportButton
            data={states}
            filename="national_state_rankings"
          />
        </div>
        <div className="data-table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th style={{ textAlign: 'left', width: 40 }}>#</th>
                <th style={{ textAlign: 'left' }}>State</th>
                <th>Handle</th>
                <th>Std GGR</th>
                <th>Hold %</th>
                <th>Tax Paid</th>
                <th>Operators</th>
                <th>YoY Handle</th>
                <th style={{ textAlign: 'left' }}>Period</th>
              </tr>
            </thead>
            <tbody>
              {states?.map((st, i) => {
                const yoyChange = formatChange(st.total_handle, st.yoy_handle);
                return (
                  <tr
                    key={st.state_code}
                    className="clickable"
                    onClick={() => onNavigateToState(st.state_code)}
                  >
                    <td style={{ textAlign: 'left', color: 'var(--text-tertiary)' }}>{i + 1}</td>
                    <td style={{ textAlign: 'left' }}>
                      <span className="color-dot" style={{ background: getStateColor(st.state_code) }} />
                      {st.state_code}
                      <span style={{ color: 'var(--text-tertiary)', marginLeft: 8, fontSize: 12, fontFamily: 'var(--font-body)' }}>
                        {st.state_name}
                      </span>
                    </td>
                    <td>
                      {st.total_handle ? formatCurrency(st.total_handle) : (
                        <MetricInfo stateCode={st.state_code} metric="handle" compact>
                          <span>{'-'}</span>
                        </MetricInfo>
                      )}
                    </td>
                    <td>
                      {st.total_ggr ? formatCurrency(st.total_ggr) : (
                        <MetricInfo stateCode={st.state_code} metric="standard_ggr" compact>
                          <span>{'-'}</span>
                        </MetricInfo>
                      )}
                    </td>
                    <td>{formatPct(st.hold_pct)}</td>
                    <td>
                      {st.total_tax ? formatCurrency(st.total_tax) : (
                        <MetricInfo stateCode={st.state_code} metric="tax_paid" compact>
                          <span>{'-'}</span>
                        </MetricInfo>
                      )}
                    </td>
                    <td>{st.num_operators || '-'}</td>
                    <td>
                      {yoyChange ? (
                        <span className={yoyChange.direction === 'up' ? 'cell-positive' : 'cell-negative'}>
                          {yoyChange.label}
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
    </div>
  );
}

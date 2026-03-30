import { useState, useMemo } from 'react';
import {
  AreaChart, Area, LineChart, Line,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';
import { useData } from '../hooks/useData';
import { getOperatorDetail, getOperatorSummary } from '../data/loader';
import { formatCurrency, formatPct, formatChange, formatAxisMonth } from '../utils/format';
import { getOperatorColor, getStateColor, STATE_NAMES } from '../utils/colors';
import ChartCard from './ChartCard';
import ExportButton from './ExportButton';
import { PageSkeleton } from './LoadingSkeleton';
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

function ChartTooltip({ active, payload, label, isCurrency = true }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="tooltip-date">{label}</div>
      {payload.filter(p => p.value != null).map((p, i) => (
        <div key={i} className="tooltip-row">
          <span className="tooltip-label" style={{ color: p.color || p.stroke }}>{p.name}</span>
          <span className="tooltip-value">
            {isCurrency ? formatCurrency(p.value) : formatPct(p.value)}
          </span>
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

export default function OperatorDetail({ operatorName, onBack }) {
  const [channel, setChannel] = useState(null);
  const [rangeMonths, setRangeMonths] = useState(12);

  const { data: detail, loading } = useData(
    () => getOperatorDetail(operatorName, channel),
    [operatorName, channel]
  );

  const { summary, stateBreakdown, timeSeries } = detail || {};

  const handleData = useMemo(() => {
    if (!timeSeries) return [];
    const filtered = filterByRange(timeSeries, rangeMonths) || timeSeries;
    return filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) }));
  }, [timeSeries, rangeMonths]);

  const holdData = useMemo(() => {
    if (!timeSeries) return [];
    const filtered = filterByRange(timeSeries, rangeMonths) || timeSeries;
    return filtered.filter(d => d.hold_pct != null).map(d => ({
      date: formatAxisMonth(d.period_end),
      period_end: d.period_end,
      hold_pct: d.hold_pct,
    }));
  }, [timeSeries, rangeMonths]);

  const color = getOperatorColor(operatorName);

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-4)' }}>
          <button className="btn" onClick={onBack}>Back</button>
          <span className="color-dot" style={{ background: color }} />
          <span className="state-name">{operatorName}</span>
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

      {loading && <PageSkeleton />}

      {summary && (
        <div className="stat-cards">
          <div className="stat-card">
            <span className="stat-label">Handle (Latest Mo)</span>
            <span className="stat-value">{formatCurrency(summary.total_handle)}</span>
          </div>
          <div className="stat-card">
            <span className="stat-label">Std GGR (Latest Mo)</span>
            <span className="stat-value">{formatCurrency(summary.total_ggr)}</span>
          </div>
          <div className="stat-card">
            <span className="stat-label">Hold %</span>
            <span className="stat-value">{formatPct(summary.hold_pct)}</span>
          </div>
          <div className="stat-card">
            <span className="stat-label">Active States</span>
            <span className="stat-value">{summary.state_count}</span>
          </div>
        </div>
      )}

      {!loading && (
        <>
          <div className="charts-row">
            <ChartCard title="Handle & GGR Over Time">
              <ResponsiveContainer width="100%" height={320}>
                <AreaChart data={handleData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                  <Tooltip content={<ChartTooltip />} />
                  <Area type="monotone" dataKey="handle" stroke={color} fill={color} fillOpacity={0.12} strokeWidth={2} name="Handle" isAnimationActive={false} />
                  <Area type="monotone" dataKey="ggr" stroke="var(--positive)" fill="var(--positive)" fillOpacity={0.12} strokeWidth={2} name="GGR" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard title="Hold % Trend">
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={holdData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} interval="preserveStartEnd" />
                  <YAxis tick={AXIS_TICK} axisLine={{ stroke: '#1a1a28' }} tickLine={false} tickFormatter={v => formatPct(v)} width={55} domain={['auto', 'auto']} />
                  <Tooltip content={<ChartTooltip isCurrency={false} />} />
                  <Line type="monotone" dataKey="hold_pct" stroke="var(--warning)" strokeWidth={2} dot={false} name="Hold %" isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>
          </div>

          {stateBreakdown?.length > 0 && (
            <div className="card">
              <div className="card-header">
                <div className="card-title">Performance by State - Latest Month</div>
                <ExportButton data={stateBreakdown} filename={`${operatorName.toLowerCase().replace(/\s+/g, '_')}_states`} />
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
                      <th>MoM Handle</th>
                      <th>YoY Handle</th>
                      <th style={{ textAlign: 'left' }}>Period</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stateBreakdown.map((st, i) => {
                      const mom = formatChange(st.handle, st.prev_handle);
                      const yoy = formatChange(st.handle, st.yoy_handle);
                      return (
                        <tr key={st.state_code}>
                          <td style={{ textAlign: 'left', color: 'var(--text-tertiary)' }}>{i + 1}</td>
                          <td style={{ textAlign: 'left' }}>
                            <span className="color-dot" style={{ background: getStateColor(st.state_code) }} />
                            {st.state_code}
                            <span style={{ color: 'var(--text-tertiary)', marginLeft: 8, fontSize: 12, fontFamily: 'var(--font-body)' }}>
                              {st.state_name}
                            </span>
                          </td>
                          <td><SourceableValue value={st.handle} formattedValue={formatCurrency(st.handle)} row={st} metric="Handle" /></td>
                          <td><SourceableValue value={st.ggr} formattedValue={formatCurrency(st.ggr)} row={st} metric="Std GGR" /></td>
                          <td>{formatPct(st.hold_pct)}</td>
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
    </div>
  );
}

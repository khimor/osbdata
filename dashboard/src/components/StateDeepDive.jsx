import { useState, useMemo } from 'react';
import {
  LineChart, Line, AreaChart, Area,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { useData } from '../hooks/useData';
import {
  getStateTimeSeries, getStateOperatorTimeSeries, getStateOperatorTable,
  getNationalSummary, getStateSportsTimeSeries, stateHasWeeklyData,
} from '../data/loader';
import { formatCurrency, formatPct, formatChange, formatDate, formatAxisMonth } from '../utils/format';
import { getOperatorColor, getSportColor, STATE_NAMES, getStateColor } from '../utils/colors';
import ChartCard from './ChartCard';
import ExportButton from './ExportButton';
import SourceableValue from './SourceableValue';
import MetricInfo from './MetricInfo';
import { PageSkeleton } from './LoadingSkeleton';

const AXIS_TICK = { fill: '#55556a', fontSize: 11, fontFamily: 'JetBrains Mono' };
const AXIS_LINE = { stroke: '#1a1a28' };
const GRID_STYLE = { stroke: '#1a1a28', strokeDasharray: 'none' };

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

const DATE_PRESETS = [
  { label: '3M', months: 3 },
  { label: '6M', months: 6 },
  { label: '1Y', months: 12 },
  { label: '2Y', months: 24 },
  { label: 'ALL', months: null },
];

function filterByRange(data, rangeMonths) {
  if (!data || !rangeMonths) return data;
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - rangeMonths);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  return data.filter(d => d.period_end >= cutoffStr);
}

const CHANNEL_OPTIONS = [
  { value: null, label: 'Combined' },
  { value: 'online', label: 'Online' },
  { value: 'retail', label: 'In-Person' },
];

export default function StateDeepDive({ stateCode: initialState }) {
  const [stateCode, setStateCode] = useState(initialState || 'NY');
  const [rangeMonths, setRangeMonths] = useState(12);
  const [selectedPeriod, setSelectedPeriod] = useState(null);
  const [channel, setChannel] = useState(null);
  const [periodType, setPeriodType] = useState('monthly');

  const { data: stateList } = useData(() => getNationalSummary(), []);
  const { data: hasWeekly } = useData(() => stateHasWeeklyData(stateCode), [stateCode]);

  const { data: timeSeries, loading: loadingTS } = useData(
    () => getStateTimeSeries(stateCode, periodType, channel), [stateCode, periodType, channel]
  );
  const { data: opTimeSeries, loading: loadingOpTS } = useData(
    () => getStateOperatorTimeSeries(stateCode, 5, channel), [stateCode, channel]
  );
  const { data: opTableData, loading: loadingOpTable } = useData(
    () => getStateOperatorTable(stateCode, selectedPeriod, channel, periodType), [stateCode, selectedPeriod, channel, periodType]
  );
  const { data: sportsData, loading: loadingSports } = useData(
    () => getStateSportsTimeSeries(stateCode, channel), [stateCode, channel]
  );

  const availablePeriods = useMemo(() => {
    if (!timeSeries) return [];
    return timeSeries.map(d => d.period_end);
  }, [timeSeries]);

  const summary = useMemo(() => {
    if (!timeSeries || !timeSeries.length) return null;

    let current;
    if (selectedPeriod) {
      current = timeSeries.find(d => d.period_end === selectedPeriod);
    }
    if (!current) {
      current = timeSeries[timeSeries.length - 1];
    }

    const currentIdx = timeSeries.indexOf(current);
    const prev = currentIdx > 0 ? timeSeries[currentIdx - 1] : null;

    let yoyRow = null;
    const d = new Date(current.period_end + 'T00:00:00');
    const yoyMonth = `${d.getFullYear() - 1}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    yoyRow = timeSeries.find(r => r.period_end.startsWith(yoyMonth));

    const yoyHoldBps = (current.hold_pct != null && yoyRow?.hold_pct != null)
      ? Math.round((current.hold_pct - yoyRow.hold_pct) * 10000)
      : null;

    return {
      handle: current.handle,
      ggr: current.standard_ggr,
      holdPct: current.hold_pct,
      tax: current.tax_paid,
      period: current.period_end,
      momHandle: formatChange(current.handle, prev?.handle),
      yoyHandle: formatChange(current.handle, yoyRow?.handle),
      yoyGgr: formatChange(current.standard_ggr, yoyRow?.standard_ggr),
      yoyTax: formatChange(current.tax_paid, yoyRow?.tax_paid),
      yoyHoldBps,
    };
  }, [timeSeries, selectedPeriod]);

  const opTableTitle = useMemo(() => {
    const period = opTableData?.period;
    if (!period) return 'Operator Summary';
    return `Operator Summary - ${formatDate(period)}`;
  }, [opTableData]);

  const stateReportsHandle = useMemo(() => {
    if (!opTableData?.operators) return true;
    const totalHandle = opTableData.operators.reduce((s, o) => s + (o.handle || 0), 0);
    return totalHandle > 0;
  }, [opTableData]);

  const isWeekly = periodType === 'weekly';

  const handleChartData = useMemo(() => {
    if (!timeSeries) return [];
    const filtered = filterByRange(timeSeries, rangeMonths) || timeSeries.slice(-36);
    return filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end, isWeekly) }));
  }, [timeSeries, rangeMonths, isWeekly]);

  const holdChartData = useMemo(() => {
    if (!timeSeries) return [];
    const filtered = filterByRange(timeSeries, rangeMonths) || timeSeries.slice(-36);
    return filtered.filter(d => d.hold_pct != null).map(d => ({
      date: formatAxisMonth(d.period_end, isWeekly),
      hold_pct: d.hold_pct,
    }));
  }, [timeSeries, rangeMonths, isWeekly]);

  const opAreaData = useMemo(() => {
    if (!opTimeSeries) return { series: [], keys: [] };
    const filtered = filterByRange(opTimeSeries.series, rangeMonths) || opTimeSeries.series.slice(-24);
    return {
      series: filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) })),
      keys: opTimeSeries.keys,
    };
  }, [opTimeSeries, rangeMonths]);

  const sportsChartData = useMemo(() => {
    if (!sportsData) return null;
    const filtered = filterByRange(sportsData.series, rangeMonths) || sportsData.series.slice(-36);
    return {
      series: filtered.map(d => ({ ...d, date: formatAxisMonth(d.period_end) })),
      keys: sportsData.keys,
    };
  }, [sportsData, rangeMonths]);

  const loading = loadingTS || loadingOpTS || loadingOpTable || loadingSports;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-4)' }}>
          <span className="color-dot" style={{ background: getStateColor(stateCode) }} />
          <span className="state-name">{STATE_NAMES[stateCode] || stateCode}</span>
          <select value={stateCode} onChange={e => { setStateCode(e.target.value); setSelectedPeriod(null); }}>
            {(stateList || []).map(st => (
              <option key={st.state_code} value={st.state_code}>
                {st.state_code} - {st.state_name}
              </option>
            ))}
          </select>
        </div>
        <div className="page-header-controls">
          {hasWeekly && (
            <div className="view-toggle" style={{ marginBottom: 0 }}>
              <button
                className={`view-toggle-btn ${periodType === 'monthly' ? 'active' : ''}`}
                onClick={() => { setPeriodType('monthly'); setSelectedPeriod(null); }}
              >
                Monthly
              </button>
              <button
                className={`view-toggle-btn ${periodType === 'weekly' ? 'active' : ''}`}
                onClick={() => { setPeriodType('weekly'); setSelectedPeriod(null); }}
              >
                Weekly
              </button>
            </div>
          )}
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
          {availablePeriods.length > 0 && (
            <select
              className="period-select"
              value={selectedPeriod || availablePeriods[availablePeriods.length - 1]}
              onChange={e => setSelectedPeriod(e.target.value)}
            >
              {[...availablePeriods].reverse().map(pe => (
                <option key={pe} value={pe}>{formatDate(pe, periodType)}</option>
              ))}
            </select>
          )}
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
            <span className="stat-label">Handle <MetricInfo stateCode={stateCode} metric="handle" /></span>
            <span className="stat-value">{formatCurrency(summary.handle)}</span>
            {summary.yoyHandle && (
              <span className={`stat-change ${summary.yoyHandle.direction === 'up' ? 'positive' : 'negative'}`}>
                {summary.yoyHandle.label} YoY
              </span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Std GGR <MetricInfo stateCode={stateCode} metric="standard_ggr" /></span>
            <span className="stat-value">{formatCurrency(summary.ggr)}</span>
            {summary.yoyGgr && (
              <span className={`stat-change ${summary.yoyGgr.direction === 'up' ? 'positive' : 'negative'}`}>
                {summary.yoyGgr.label} YoY
              </span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Hold %</span>
            <span className="stat-value">{formatPct(summary.holdPct)}</span>
            {summary.yoyHoldBps != null && (
              <span className={`stat-change ${summary.yoyHoldBps >= 0 ? 'positive' : 'negative'}`}>
                {summary.yoyHoldBps >= 0 ? '+' : ''}{summary.yoyHoldBps} bps YoY
              </span>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Tax Paid <MetricInfo stateCode={stateCode} metric="tax_paid" /></span>
            <span className="stat-value">{formatCurrency(summary.tax)}</span>
            {summary.yoyTax && (
              <span className={`stat-change ${summary.yoyTax.direction === 'up' ? 'positive' : 'negative'}`}>
                {summary.yoyTax.label} YoY
              </span>
            )}
          </div>
        </div>
      )}

      {!loading && (
        <>
          <div className="charts-row">
            <ChartCard title="Handle Over Time">
              <ResponsiveContainer width="100%" height={320}>
                <AreaChart data={handleChartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} interval="preserveStartEnd" />
                  <YAxis tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                  <Tooltip content={<ChartTooltip />} />
                  <Area
                    type="monotone" dataKey="handle"
                    stroke="var(--accent-primary)" fill="var(--accent-primary)"
                    fillOpacity={0.12} strokeWidth={2}
                    name="Handle" isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard title="Hold % Trend">
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={holdChartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid {...GRID_STYLE} vertical={false} />
                  <XAxis dataKey="date" tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} interval="preserveStartEnd" />
                  <YAxis tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} tickFormatter={v => formatPct(v)} width={55} domain={['auto', 'auto']} />
                  <Tooltip content={<ChartTooltip isCurrency={false} />} />
                  <Line
                    type="monotone" dataKey="hold_pct"
                    stroke="var(--warning)" strokeWidth={2}
                    dot={false} name="Hold %"
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>
          </div>

          {opAreaData.keys.length > 1 && (
            <div className="charts-row single">
              <ChartCard title="Operator Market Share Over Time (Handle)">
                <ResponsiveContainer width="100%" height={320}>
                  <AreaChart data={opAreaData.series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                    <CartesianGrid {...GRID_STYLE} vertical={false} />
                    <XAxis dataKey="date" tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} interval="preserveStartEnd" />
                    <YAxis tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                    <Tooltip content={<ChartTooltip />} />
                    <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                    {opAreaData.keys.map(key => (
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
          )}

          {sportsChartData && sportsChartData.keys.length > 0 && (
            <div className="charts-row single">
              <ChartCard title="Handle by Sport">
                <ResponsiveContainer width="100%" height={320}>
                  <AreaChart data={sportsChartData.series} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                    <CartesianGrid {...GRID_STYLE} vertical={false} />
                    <XAxis dataKey="date" tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} interval="preserveStartEnd" />
                    <YAxis tick={AXIS_TICK} axisLine={AXIS_LINE} tickLine={false} tickFormatter={v => formatCurrency(v)} width={70} />
                    <Tooltip content={<ChartTooltip />} />
                    <Legend wrapperStyle={{ fontSize: 12, fontFamily: 'Instrument Sans' }} />
                    {sportsChartData.keys.map(key => (
                      <Area
                        key={key} type="monotone" dataKey={key}
                        stackId="1" stroke={getSportColor(key)}
                        fill={getSportColor(key)} fillOpacity={0.6}
                        name={key} isAnimationActive={false}
                      />
                    ))}
                  </AreaChart>
                </ResponsiveContainer>
              </ChartCard>
            </div>
          )}

          {opTableData?.operators?.length > 0 && (
            <div className="card" style={{ marginBottom: 'var(--space-4)' }}>
              <div className="card-header">
                <div className="card-title">{opTableTitle}</div>
                <ExportButton data={opTableData.operators} filename={`${stateCode.toLowerCase()}_operators`} />
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left' }}>Operator</th>
                      <th>Handle <MetricInfo stateCode={stateCode} metric="handle" /></th>
                      <th>Std GGR <MetricInfo stateCode={stateCode} metric="standard_ggr" /></th>
                      <th>Hold %</th>
                      <th>Market Share</th>
                      <th>YoY Handle</th>
                    </tr>
                  </thead>
                  <tbody>
                    {opTableData.operators.map(op => {
                      const yoy = formatChange(op.handle, op.yoy_handle);
                      return (
                        <tr key={op.operator}>
                          <td style={{ textAlign: 'left' }}>
                            <span className="color-dot" style={{ background: getOperatorColor(op.operator) }} />
                            {op.operator}
                          </td>
                          <td><SourceableValue value={op.handle} formattedValue={formatCurrency(op.handle)} row={op} metric="Handle" /></td>
                          <td><SourceableValue value={op.standard_ggr} formattedValue={formatCurrency(op.standard_ggr)} row={op} metric="Std GGR" /></td>
                          <td><SourceableValue value={op.hold_pct} formattedValue={formatPct(op.hold_pct)} row={op} metric="Hold %" /></td>
                          <td>{formatPct(op.market_share)}</td>
                          <td>
                            {yoy ? (
                              <span className={yoy.direction === 'up' ? 'cell-positive' : 'cell-negative'}>
                                {yoy.label}
                              </span>
                            ) : '-'}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {opTableData?.operators?.length > 0 && !stateReportsHandle && (
            <div className="card">
              <div className="card-header">
                <div className="card-title">Standard GGR Breakdown - {formatDate(opTableData.period)}</div>
                <ExportButton data={opTableData.operators} filename={`${stateCode.toLowerCase()}_ggr_breakdown`} />
              </div>
              <div className="data-table-wrapper">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left' }}>Operator</th>
                      <th>Handle</th>
                      <th>Payouts</th>
                      <th>Std GGR</th>
                      <th>Reported GGR</th>
                      <th>Difference</th>
                      <th>Promos</th>
                      <th>Net Revenue</th>
                      <th>Tax</th>
                    </tr>
                  </thead>
                  <tbody>
                    {opTableData.operators.map(op => {
                      const diff = (op.standard_ggr != null && op.gross_revenue != null)
                        ? op.standard_ggr - op.gross_revenue
                        : null;
                      return (
                        <tr key={op.operator}>
                          <td style={{ textAlign: 'left' }}>
                            <span className="color-dot" style={{ background: getOperatorColor(op.operator) }} />
                            {op.operator}
                          </td>
                          <td><SourceableValue value={op.handle} formattedValue={formatCurrency(op.handle)} row={op} metric="Handle" /></td>
                          <td><SourceableValue value={op.payouts} formattedValue={formatCurrency(op.payouts)} row={op} metric="Payouts" /></td>
                          <td><SourceableValue value={op.standard_ggr} formattedValue={formatCurrency(op.standard_ggr)} row={op} metric="Std GGR" /></td>
                          <td><SourceableValue value={op.gross_revenue} formattedValue={formatCurrency(op.gross_revenue)} row={op} metric="Reported GGR" /></td>
                          <td className={diff > 0 ? 'cell-positive' : diff < 0 ? 'cell-negative' : ''}>
                            {diff != null ? formatCurrency(diff) : '-'}
                          </td>
                          <td><SourceableValue value={op.promo_credits} formattedValue={formatCurrency(op.promo_credits)} row={op} metric="Promos" /></td>
                          <td><SourceableValue value={op.net_revenue} formattedValue={formatCurrency(op.net_revenue)} row={op} metric="Net Revenue" /></td>
                          <td><SourceableValue value={op.tax_paid} formattedValue={formatCurrency(op.tax_paid)} row={op} metric="Tax" /></td>
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

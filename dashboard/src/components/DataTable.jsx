import { useState, useMemo, useCallback } from 'react';
import { useData } from '../hooks/useData';
import { getAllData } from '../data/loader';
import { formatCurrency, formatPct, formatDate, formatNumber } from '../utils/format';
import ExportButton from './ExportButton';
import SourceableValue from './SourceableValue';
import { PageSkeleton } from './LoadingSkeleton';

const PAGE_SIZE = 100;

const MONEY_COLS = ['handle', 'gross_revenue', 'standard_ggr', 'promo_credits', 'net_revenue', 'payouts', 'tax_paid', 'federal_excise_tax'];
const PCT_COLS = ['hold_pct'];

const OPERATOR_COLS = [
  'state_code', 'period_end', 'period_type', 'operator_reported', 'operator_standard',
  'parent_company', 'channel',
  'handle', 'gross_revenue', 'standard_ggr', 'promo_credits', 'net_revenue',
  'tax_paid', 'hold_pct',
];

const SPORT_COLS = [
  'state_code', 'period_end', 'period_type', 'sport_category', 'channel',
  'handle', 'gross_revenue', 'standard_ggr', 'payouts', 'hold_pct',
];

function formatCell(col, value, row) {
  if (value == null || value === '') return '-';
  if (MONEY_COLS.includes(col)) return formatCurrency(value);
  if (PCT_COLS.includes(col)) return formatPct(value);
  if (col === 'period_end' || col === 'period_start') return formatDate(value, row?.period_type);
  return String(value);
}

function isNumericCol(col) {
  return MONEY_COLS.includes(col) || PCT_COLS.includes(col);
}

export default function DataTable() {
  const { data: allData, loading, error } = useData(() => getAllData(), []);

  const [viewMode, setViewMode] = useState('operators');
  const [sortCol, setSortCol] = useState('period_end');
  const [sortDir, setSortDir] = useState('desc');
  const [page, setPage] = useState(0);
  const [filters, setFilters] = useState({
    state: '',
    operator: '',
    channel: '',
    periodType: '',
    search: '',
  });

  const DISPLAY_COLS = viewMode === 'sports' ? SPORT_COLS : OPERATOR_COLS;

  const filterOptions = useMemo(() => {
    if (!allData) return { states: [], operators: [], channels: [], periodTypes: [] };
    const states = [...new Set(allData.map(r => r.state_code))].sort();
    const operators = [...new Set(allData.map(r => r.operator_standard).filter(Boolean))].sort();
    const channels = [...new Set(allData.map(r => r.channel).filter(Boolean))].sort();
    const periodTypes = [...new Set(allData.map(r => r.period_type).filter(Boolean))].sort();
    return { states, operators, channels, periodTypes };
  }, [allData]);

  const filtered = useMemo(() => {
    if (!allData) return [];
    let result = allData;

    if (viewMode === 'operators') {
      result = result.filter(r => !r.sport_category);
    } else {
      result = result.filter(r => r.sport_category);
    }

    if (filters.state) result = result.filter(r => r.state_code === filters.state);
    if (filters.operator) result = result.filter(r => r.operator_standard === filters.operator);
    if (filters.channel) result = result.filter(r => r.channel === filters.channel);
    if (filters.periodType) result = result.filter(r => r.period_type === filters.periodType);
    if (filters.search) {
      const q = filters.search.toLowerCase();
      result = result.filter(r =>
        (r.operator_standard || '').toLowerCase().includes(q) ||
        (r.operator_reported || '').toLowerCase().includes(q) ||
        (r.sport_category || '').toLowerCase().includes(q) ||
        (r.state_code || '').toLowerCase().includes(q)
      );
    }
    return result;
  }, [allData, filters, viewMode]);

  const sorted = useMemo(() => {
    const data = [...filtered];
    data.sort((a, b) => {
      let va = a[sortCol];
      let vb = b[sortCol];
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === 'number' && typeof vb === 'number') {
        return sortDir === 'asc' ? va - vb : vb - va;
      }
      va = String(va);
      vb = String(vb);
      return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    });
    return data;
  }, [filtered, sortCol, sortDir]);

  const pageData = useMemo(() => {
    const start = page * PAGE_SIZE;
    return sorted.slice(start, start + PAGE_SIZE);
  }, [sorted, page]);

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);

  const handleSort = useCallback((col) => {
    if (col === sortCol) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortCol(col);
      setSortDir(isNumericCol(col) ? 'desc' : 'asc');
    }
    setPage(0);
  }, [sortCol]);

  const updateFilter = useCallback((key, value) => {
    setFilters(f => ({ ...f, [key]: value }));
    setPage(0);
  }, []);

  if (loading) return <PageSkeleton />;
  if (error) return <div className="error-state">{error}</div>;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h2 className="page-title">Data Table</h2>
          <div className="page-subtitle">
            {formatNumber(sorted.length)} rows across {filterOptions.states.length} states
          </div>
        </div>
        <ExportButton
          data={filtered}
          filename={`osb_${viewMode}_${filters.state || 'all'}`}
        />
      </div>

      <div className="view-toggle">
        <button
          className={`view-toggle-btn ${viewMode === 'operators' ? 'active' : ''}`}
          onClick={() => { setViewMode('operators'); setPage(0); }}
        >
          Operators
        </button>
        <button
          className={`view-toggle-btn ${viewMode === 'sports' ? 'active' : ''}`}
          onClick={() => { setViewMode('sports'); setPage(0); }}
        >
          Sports
        </button>
      </div>

      <div className="filter-bar">
        <div className="filter-group">
          <label>State</label>
          <select value={filters.state} onChange={e => updateFilter('state', e.target.value)}>
            <option value="">All States</option>
            {filterOptions.states.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        {viewMode === 'operators' && (
          <>
            <div className="divider" />
            <div className="filter-group">
              <label>Operator</label>
              <select value={filters.operator} onChange={e => updateFilter('operator', e.target.value)}>
                <option value="">All Operators</option>
                {filterOptions.operators.map(o => <option key={o} value={o}>{o}</option>)}
              </select>
            </div>
          </>
        )}
        <div className="divider" />
        <div className="filter-group">
          <label>Channel</label>
          <select value={filters.channel} onChange={e => updateFilter('channel', e.target.value)}>
            <option value="">All Channels</option>
            {filterOptions.channels.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <div className="divider" />
        <div className="filter-group">
          <label>Period</label>
          <select value={filters.periodType} onChange={e => updateFilter('periodType', e.target.value)}>
            <option value="">All</option>
            {filterOptions.periodTypes.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
        <div className="divider" />
        <div className="filter-group">
          <label>Search</label>
          <input
            type="text"
            placeholder="Search..."
            value={filters.search}
            onChange={e => updateFilter('search', e.target.value)}
            style={{ minWidth: 140 }}
          />
        </div>
      </div>

      <div className="row-count">
        Showing {formatNumber(Math.min(PAGE_SIZE, sorted.length - page * PAGE_SIZE))} of {formatNumber(sorted.length)} rows
        {sorted.length !== (allData?.length || 0) && ` (filtered from ${formatNumber(allData?.length || 0)})`}
      </div>

      <div className="card" style={{ padding: 0 }}>
        <div className="data-table-wrapper" style={{ maxHeight: 'calc(100vh - 360px)', overflow: 'auto' }}>
          <table className="data-table">
            <thead>
              <tr>
                {DISPLAY_COLS.map(col => {
                  const numeric = isNumericCol(col);
                  return (
                    <th
                      key={col}
                      style={!numeric ? { textAlign: 'left' } : undefined}
                      onClick={() => handleSort(col)}
                    >
                      {col.replace(/_/g, ' ')}
                      {sortCol === col && (
                        <span className="sort-arrow">{sortDir === 'asc' ? ' \u2191' : ' \u2193'}</span>
                      )}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {pageData.map((row, i) => (
                <tr key={`${row.state_code}-${row.period_end}-${row.operator_standard}-${row.channel}-${i}`}>
                  {DISPLAY_COLS.map(col => {
                    const numeric = isNumericCol(col);
                    const isMoney = MONEY_COLS.includes(col);
                    const isPct = PCT_COLS.includes(col);
                    return (
                      <td
                        key={col}
                        style={!numeric ? { textAlign: 'left', fontFamily: 'var(--font-body)' } : undefined}
                      >
                        {(isMoney || isPct) ? (
                          <SourceableValue
                            value={row[col]}
                            formattedValue={isMoney ? formatCurrency(row[col]) : formatPct(row[col])}
                            row={row}
                            metric={col.replace(/_/g, ' ')}
                          />
                        ) : (
                          formatCell(col, row[col], row)
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {totalPages > 1 && (
        <div className="pagination">
          <div className="info">
            Page {page + 1} of {formatNumber(totalPages)}
          </div>
          <div className="controls">
            <button className="btn" disabled={page === 0} onClick={() => setPage(0)}>First</button>
            <button className="btn" disabled={page === 0} onClick={() => setPage(p => Math.max(0, p - 1))}>Prev</button>
            <button className="btn" disabled={page >= totalPages - 1} onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}>Next</button>
            <button className="btn" disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)}>Last</button>
          </div>
        </div>
      )}
    </div>
  );
}

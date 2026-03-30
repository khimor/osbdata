import { useState, useMemo } from 'react';
import { ArrowRight } from 'lucide-react';
import { useData } from '../hooks/useData';
import { loadAllData } from '../data/loader';
import { formatCurrency, formatPct, formatDate } from '../utils/format';
import { getStateColor, STATE_NAMES } from '../utils/colors';
import { PageSkeleton } from './LoadingSkeleton';

const PERIOD_FILTERS = [
  { value: 'all', label: 'All' },
  { value: 'weekly', label: 'Weekly' },
  { value: 'monthly', label: 'Monthly' },
];

const PAGE_SIZE = 30;

function timeAgo(timestamp) {
  if (!timestamp) return '';
  const now = new Date();
  const then = new Date(timestamp);
  const diff = Math.floor((now - then) / 1000);

  if (diff < 60) return 'Just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 172800) return 'Yesterday';
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;
  const weeks = Math.floor(diff / 604800);
  return weeks === 1 ? '1 week ago' : `${weeks} weeks ago`;
}

export default function FeedPage({ onNavigateToState }) {
  const { data: allData, loading } = useData(() => loadAllData(), []);
  const [periodFilter, setPeriodFilter] = useState('all');
  const [stateSearch, setStateSearch] = useState('');
  const [showCount, setShowCount] = useState(PAGE_SIZE);

  const feedEvents = useMemo(() => {
    if (!allData) return [];

    // Group by state + scrape_timestamp (rounded to minute for dedup)
    const eventMap = {};
    for (const row of allData) {
      if (!row.scrape_timestamp || row.sport_category) continue;
      if (periodFilter !== 'all' && row.period_type !== periodFilter) continue;

      const tsKey = row.scrape_timestamp.slice(0, 16); // round to minute
      const key = `${row.state_code}_${row.period_end}_${tsKey}`;

      if (!eventMap[key]) {
        eventMap[key] = {
          state_code: row.state_code,
          period_end: row.period_end,
          period_type: row.period_type,
          scrape_timestamp: row.scrape_timestamp,
          handle: 0,
          ggr: 0,
          operators: new Set(),
          _hasOps: false,
          _totalHandle: 0,
          _totalGgr: 0,
        };
      }

      const ev = eventMap[key];
      const isTotalRow = ['TOTAL', 'ALL'].includes(row.operator_standard);

      if (isTotalRow) {
        ev._totalHandle += row.handle || 0;
        ev._totalGgr += row.standard_ggr ?? row.gross_revenue ?? 0;
      } else {
        ev._hasOps = true;
        ev.handle += row.handle || 0;
        ev.ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
        if (row.operator_standard && row.operator_standard !== 'UNKNOWN') {
          ev.operators.add(row.operator_standard);
        }
      }
    }

    // Finalize: use TOTAL fallback if no operator rows
    const events = Object.values(eventMap).map(ev => {
      if (ev.handle === 0 && ev._totalHandle > 0) ev.handle = ev._totalHandle;
      if (ev.ggr === 0 && ev._totalGgr > 0) ev.ggr = ev._totalGgr;
      return {
        ...ev,
        hold_pct: ev.handle > 0 ? ev.ggr / ev.handle : null,
        operator_count: ev.operators.size,
      };
    });

    // Filter by state search
    let filtered = events;
    if (stateSearch) {
      const q = stateSearch.toLowerCase();
      filtered = events.filter(ev =>
        ev.state_code.toLowerCase().includes(q) ||
        (STATE_NAMES[ev.state_code] || '').toLowerCase().includes(q)
      );
    }

    // Sort by scrape_timestamp descending (most recent first)
    filtered.sort((a, b) => (b.scrape_timestamp || '').localeCompare(a.scrape_timestamp || ''));

    // Deduplicate: keep only the latest scrape per (state, period_end)
    const seen = new Set();
    const deduped = [];
    for (const ev of filtered) {
      const dedupKey = `${ev.state_code}_${ev.period_end}`;
      if (!seen.has(dedupKey)) {
        seen.add(dedupKey);
        deduped.push(ev);
      }
    }

    return deduped;
  }, [allData, periodFilter, stateSearch]);

  const visibleEvents = feedEvents.slice(0, showCount);

  if (loading) return <PageSkeleton />;

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h2 className="page-title">Feed</h2>
          <div className="page-subtitle">Latest data releases across all states</div>
        </div>
        <div className="page-header-controls">
          <div className="view-toggle" style={{ marginBottom: 0 }}>
            {PERIOD_FILTERS.map(f => (
              <button
                key={f.value}
                className={`view-toggle-btn ${periodFilter === f.value ? 'active' : ''}`}
                onClick={() => setPeriodFilter(f.value)}
              >
                {f.label}
              </button>
            ))}
          </div>
          <input
            type="text"
            placeholder="Search state..."
            value={stateSearch}
            onChange={e => setStateSearch(e.target.value)}
            style={{ minWidth: 140 }}
          />
        </div>
      </div>

      <div className="feed-timeline">
        {visibleEvents.map((ev, i) => (
          <div
            key={`${ev.state_code}_${ev.period_end}_${i}`}
            className="feed-card"
            onClick={() => onNavigateToState?.(ev.state_code)}
          >
            <div className="feed-card-header">
              <div className="feed-card-state">
                <span className="color-dot" style={{ background: getStateColor(ev.state_code) }} />
                <span className="feed-card-code">{ev.state_code}</span>
                <span className="feed-card-name">{STATE_NAMES[ev.state_code]}</span>
              </div>
              <span className="feed-card-time">{timeAgo(ev.scrape_timestamp)}</span>
            </div>
            <div className="feed-card-period">
              {ev.period_type === 'weekly' ? 'Weekly' : 'Monthly'} data through {formatDate(ev.period_end, ev.period_type)}
            </div>
            <div className="feed-card-metrics">
              {ev.handle > 0 && (
                <div className="feed-metric">
                  <span className="feed-metric-label">Handle</span>
                  <span className="feed-metric-value">{formatCurrency(ev.handle)}</span>
                </div>
              )}
              {ev.ggr !== 0 && (
                <div className="feed-metric">
                  <span className="feed-metric-label">GGR</span>
                  <span className="feed-metric-value">{formatCurrency(ev.ggr)}</span>
                </div>
              )}
              {ev.hold_pct != null && (
                <div className="feed-metric">
                  <span className="feed-metric-label">Hold</span>
                  <span className="feed-metric-value">{formatPct(ev.hold_pct)}</span>
                </div>
              )}
              {ev.operator_count > 0 && (
                <div className="feed-metric">
                  <span className="feed-metric-label">Operators</span>
                  <span className="feed-metric-value">{ev.operator_count}</span>
                </div>
              )}
            </div>
            <div className="feed-card-action">
              View State <ArrowRight size={14} />
            </div>
          </div>
        ))}
      </div>

      {showCount < feedEvents.length && (
        <div style={{ textAlign: 'center', padding: 'var(--space-6) 0' }}>
          <button className="btn" onClick={() => setShowCount(c => c + PAGE_SIZE)}>
            Load More ({feedEvents.length - showCount} remaining)
          </button>
        </div>
      )}

      {feedEvents.length === 0 && !loading && (
        <div className="loading-state">No data events found</div>
      )}
    </div>
  );
}

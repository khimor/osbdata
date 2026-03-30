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

    // Step 1: Find the latest period_end per state (one entry per state)
    const stateLatest = {};
    for (const row of allData) {
      if (!row.period_end || row.sport_category) continue;
      if (periodFilter !== 'all' && row.period_type !== periodFilter) continue;

      const sc = row.state_code;
      if (!stateLatest[sc] || row.period_end > stateLatest[sc]) {
        stateLatest[sc] = row.period_end;
      }
    }

    // Step 2: Aggregate metrics for each state's latest period
    const eventMap = {};
    for (const row of allData) {
      if (row.sport_category) continue;
      const sc = row.state_code;
      if (!stateLatest[sc] || row.period_end !== stateLatest[sc]) continue;
      if (periodFilter !== 'all' && row.period_type !== periodFilter) continue;

      if (!eventMap[sc]) {
        eventMap[sc] = {
          state_code: sc,
          period_end: row.period_end,
          period_type: row.period_type,
          scrape_timestamp: row.scrape_timestamp,
          handle: 0,
          ggr: 0,
          operators: new Set(),
          _totalHandle: 0,
          _totalGgr: 0,
        };
      }

      const ev = eventMap[sc];
      // Keep the most recent scrape_timestamp
      if (row.scrape_timestamp && (!ev.scrape_timestamp || row.scrape_timestamp > ev.scrape_timestamp)) {
        ev.scrape_timestamp = row.scrape_timestamp;
      }

      const isTotalRow = ['TOTAL', 'ALL'].includes(row.operator_standard);
      if (isTotalRow) {
        ev._totalHandle += row.handle || 0;
        ev._totalGgr += row.standard_ggr ?? row.gross_revenue ?? 0;
      } else {
        ev.handle += row.handle || 0;
        ev.ggr += row.standard_ggr ?? row.gross_revenue ?? 0;
        if (row.operator_standard && !['UNKNOWN'].includes(row.operator_standard)) {
          ev.operators.add(row.operator_standard);
        }
      }
    }

    // Finalize
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

    // Sort by scrape_timestamp descending (most recently updated states first)
    filtered.sort((a, b) => (b.scrape_timestamp || '').localeCompare(a.scrape_timestamp || ''));

    return filtered;
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

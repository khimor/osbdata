/**
 * Skeleton loading placeholders that match the page layouts.
 * Shows pulsing gray blocks instead of blank space while data loads.
 */

function SkeletonBlock({ width = '100%', height = 16, style = {} }) {
  return (
    <div className="skeleton-block" style={{ width, height, ...style }} />
  );
}

export function StatCardsSkeleton({ count = 4 }) {
  return (
    <div className="stat-cards">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="stat-card">
          <SkeletonBlock width={80} height={10} />
          <SkeletonBlock width={120} height={28} style={{ marginTop: 8 }} />
          <SkeletonBlock width={60} height={12} style={{ marginTop: 6 }} />
        </div>
      ))}
    </div>
  );
}

export function ChartSkeleton() {
  return (
    <div className="chart-card">
      <div className="chart-header">
        <SkeletonBlock width={160} height={11} />
      </div>
      <SkeletonBlock width="100%" height={320} style={{ borderRadius: 4 }} />
    </div>
  );
}

export function TableSkeleton({ rows = 8, cols = 6 }) {
  return (
    <div className="card">
      <div className="card-header">
        <SkeletonBlock width={200} height={11} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        {Array.from({ length: rows }).map((_, i) => (
          <div key={i} style={{ display: 'flex', gap: 12, padding: '8px 12px' }}>
            {Array.from({ length: cols }).map((_, j) => (
              <SkeletonBlock key={j} width={j === 0 ? 100 : 70} height={14} />
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

export function PageSkeleton() {
  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <SkeletonBlock width={200} height={24} />
        <SkeletonBlock width={280} height={13} style={{ marginTop: 6 }} />
      </div>
      <StatCardsSkeleton />
      <div className="charts-row" style={{ marginTop: 24 }}>
        <ChartSkeleton />
        <ChartSkeleton />
      </div>
      <div style={{ marginTop: 24 }}>
        <TableSkeleton />
      </div>
    </div>
  );
}

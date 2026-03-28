export default function ChartCard({ title, children, action }) {
  return (
    <div className="chart-card">
      <div className="chart-header">
        <div className="chart-title">{title}</div>
        {action && <div>{action}</div>}
      </div>
      {children}
    </div>
  );
}

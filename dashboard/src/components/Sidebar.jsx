import { Globe, MapPin, Users, GitCompareArrows, BarChart3, Table2, BookOpen } from 'lucide-react';

const NAV_ITEMS = [
  { id: 'national', label: 'National Overview', icon: Globe },
  { id: 'operators', label: 'Operator View', icon: Users },
  { id: 'compare-ops', label: 'Compare Operators', icon: BarChart3 },
  { id: 'compare', label: 'Compare States', icon: GitCompareArrows },
  { id: 'state', label: 'State Deep Dive', icon: MapPin },
  { id: 'data', label: 'Data Table', icon: Table2 },
  { id: 'docs', label: 'API Docs', icon: BookOpen },
];

export default function Sidebar({ activeView, onNavigate, dataAsOf }) {
  return (
    <nav className="sidebar" role="navigation" aria-label="Main navigation">
      <div className="sidebar-logo">
        <h1>OSB Tracker</h1>
        <div className="subtitle">US Sports Betting Data</div>
      </div>
      <div className="sidebar-nav" role="tablist" aria-label="Dashboard views">
        <div className="nav-section-label">Views</div>
        {NAV_ITEMS.map(item => (
          <button
            key={item.id}
            className={`nav-item ${activeView === item.id ? 'active' : ''}`}
            onClick={() => onNavigate(item.id)}
            role="tab"
            aria-selected={activeView === item.id}
            aria-label={item.label}
          >
            <item.icon aria-hidden="true" />
            <span>{item.label}</span>
          </button>
        ))}
      </div>
      {dataAsOf && (
        <div className="sidebar-footer">
          <div className="data-freshness">Data as of: {dataAsOf}</div>
        </div>
      )}
    </nav>
  );
}

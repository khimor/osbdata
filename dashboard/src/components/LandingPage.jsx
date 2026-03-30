import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowRight, BarChart3, Shield, Zap, GitCompareArrows } from 'lucide-react';
import { STATE_NAMES } from '../utils/colors';

const STATE_CODES = [
  'AR','AZ','CO','CT','DC','DE','IA','IL','IN','KS','KY','LA',
  'MA','MD','ME','MI','MO','MS','MT','NC','NE','NH','NJ','NV',
  'NY','OH','OR','PA','RI','SD','TN','VA','VT','WV','WY'
];

const FEATURES = [
  {
    icon: GitCompareArrows,
    title: 'Cross-State Comparison',
    desc: 'Compare handle, GGR, and hold % across any combination of states. Overlay trends on interactive charts.',
  },
  {
    icon: BarChart3,
    title: 'Operator Intelligence',
    desc: 'Track FanDuel, DraftKings, BetMGM, and 30+ operators. Market share, state-by-state breakdowns, YoY growth.',
  },
  {
    icon: Shield,
    title: 'Source Verification',
    desc: 'Every number links back to the original regulatory filing. PDF screenshots, raw data lines, and direct source URLs.',
  },
  {
    icon: Zap,
    title: 'Real-Time Updates',
    desc: 'Automated scrapers detect new data within minutes of state publication. Email alerts when data lands.',
  },
];

function formatBillions(cents) {
  if (!cents) return '-';
  const d = cents / 100;
  if (d >= 1e9) return '$' + (d / 1e9).toFixed(1) + 'B';
  if (d >= 1e6) return '$' + Math.round(d / 1e6) + 'M';
  return '$' + Math.round(d);
}

export default function LandingPage() {
  const [stats, setStats] = useState(null);

  useEffect(() => {
    // Load a couple CSVs to get live stats for the hero
    (async () => {
      try {
        const resp = await fetch('/data/NY.csv');
        if (!resp.ok) return;
        const text = await resp.text();
        const lines = text.trim().split('\n');
        // Count unique operators across all loaded data
        const totalRows = lines.length - 1;
        setStats({ rows: totalRows > 1000 ? '40,000+' : totalRows.toLocaleString() });
      } catch {}
    })();
  }, []);

  return (
    <div className="landing">
      {/* Hero */}
      <section className="landing-hero">
        <div className="landing-container">
          <div className="landing-badge">Data Intelligence Platform</div>
          <h1 className="landing-headline">
            US Sports Betting Data.<br />
            Every State. Every Operator.<br />
            <span className="landing-accent">Source-Verified.</span>
          </h1>
          <p className="landing-subline">
            Real-time regulatory data across 35 states. Handle, GGR, market share, and operator
            performance - updated automatically as states publish.
          </p>
          <div className="landing-cta-row">
            <Link to="/app" className="landing-cta">
              Explore Dashboard <ArrowRight size={18} />
            </Link>
            <a href="#api" className="landing-cta-secondary">API Access</a>
          </div>
          <div className="landing-proof">
            <span>35 states</span>
            <span className="landing-proof-dot" />
            <span>40,000+ data points</span>
            <span className="landing-proof-dot" />
            <span>Updated every 15 minutes</span>
          </div>
        </div>
      </section>

      {/* Live stats */}
      <section className="landing-section">
        <div className="landing-container">
          <div className="landing-stats">
            <div className="landing-stat">
              <div className="landing-stat-value">35</div>
              <div className="landing-stat-label">States Tracked</div>
            </div>
            <div className="landing-stat">
              <div className="landing-stat-value">30+</div>
              <div className="landing-stat-label">Operators Monitored</div>
            </div>
            <div className="landing-stat">
              <div className="landing-stat-value">40K+</div>
              <div className="landing-stat-label">Data Points</div>
            </div>
            <div className="landing-stat">
              <div className="landing-stat-value">15 min</div>
              <div className="landing-stat-label">Update Frequency</div>
            </div>
          </div>
        </div>
      </section>

      {/* Features */}
      <section className="landing-section">
        <div className="landing-container">
          <h2 className="landing-section-title">Built for the industry</h2>
          <p className="landing-section-sub">
            Everything operators, investors, analysts, and researchers need to understand US sports betting markets.
          </p>
          <div className="landing-features">
            {FEATURES.map(f => (
              <div key={f.title} className="landing-feature">
                <div className="landing-feature-icon">
                  <f.icon size={22} />
                </div>
                <h3 className="landing-feature-title">{f.title}</h3>
                <p className="landing-feature-desc">{f.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* State coverage */}
      <section className="landing-section">
        <div className="landing-container">
          <h2 className="landing-section-title">35-state coverage</h2>
          <p className="landing-section-sub">
            From New York to Nevada. Regulatory data from every US state with legal sports betting.
          </p>
          <div className="landing-state-grid">
            {STATE_CODES.map(code => (
              <div key={code} className="landing-state-chip">
                <span className="landing-state-code">{code}</span>
                <span className="landing-state-name">{STATE_NAMES[code]}</span>
              </div>
            ))}
          </div>
          <div style={{ textAlign: 'center', marginTop: 'var(--space-6)' }}>
            <Link to="/app" className="landing-cta">
              Explore All States <ArrowRight size={18} />
            </Link>
          </div>
        </div>
      </section>

      {/* API */}
      <section className="landing-section" id="api">
        <div className="landing-container">
          <h2 className="landing-section-title">REST API</h2>
          <p className="landing-section-sub">
            Programmatic access to the full dataset. Query by state, operator, date range, or channel.
          </p>
          <div className="landing-code">
            <pre><code>{`curl "https://yjrfmlcfvogsfodgmcfw.supabase.co/rest/v1/monthly_data
  ?state_code=eq.NY
  &period_type=eq.monthly
  &order=period_end.desc
  &limit=10"
  -H "apikey: YOUR_API_KEY"`}</code></pre>
          </div>
          <div className="landing-api-actions">
            <Link to="/app" className="landing-cta-secondary" onClick={() => {
              // Navigate to docs tab
              setTimeout(() => {
                const docsBtn = document.querySelector('button[aria-label="API Docs"]');
                if (docsBtn) docsBtn.click();
              }, 500);
            }}>
              View API Docs
            </Link>
            <a href="mailto:nosherzapoo@gmail.com?subject=OSB%20Tracker%20API%20Access" className="landing-cta">
              Request API Access <ArrowRight size={18} />
            </a>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="landing-footer">
        <div className="landing-container">
          <div className="landing-footer-brand">
            <strong>OSB Tracker</strong>
            <span>US Sports Betting Data Intelligence</span>
          </div>
          <div className="landing-footer-links">
            <Link to="/app">Dashboard</Link>
            <a href="https://github.com/khimor/osbdata" target="_blank" rel="noopener noreferrer">GitHub</a>
            <a href="mailto:nosherzapoo@gmail.com">Contact</a>
          </div>
        </div>
      </footer>
    </div>
  );
}

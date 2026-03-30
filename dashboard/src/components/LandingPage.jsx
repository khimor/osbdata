import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { ArrowRight, BarChart3, Shield, Zap, GitCompareArrows, Send } from 'lucide-react';
import { STATE_NAMES } from '../utils/colors';
import { supabase } from '../data/supabase';

const STATE_CODES = [
  'AR','AZ','CO','CT','DC','DE','IA','IL','IN','KS','KY','LA',
  'MA','MD','ME','MI','MO','MS','MT','NC','NE','NH','NJ','NV',
  'NY','OH','OR','PA','RI','SD','TN','VA','VT','WV','WY'
];

const FEATURES = [
  {
    icon: Zap,
    title: 'Fastest in Market',
    desc: 'Live detection picks up new state filings the moment they publish. Data flows to the dashboard automatically - no manual updates, no delays.',
  },
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
];

export default function LandingPage() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({ name: '', email: '', company: '', message: '' });
  const [formStatus, setFormStatus] = useState(null); // null | 'sending' | 'sent' | 'error'

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!formData.email) return;
    setFormStatus('sending');
    try {
      await supabase.from('subscribers').upsert({
        email: formData.email,
        name: formData.name || null,
        states: '"all"',
        frequency: 'immediate',
        active: true,
      }, { onConflict: 'email' });
      setFormStatus('sent');
      setFormData({ name: '', email: '', company: '', message: '' });
    } catch {
      setFormStatus('error');
    }
  };

  const goToState = (code) => {
    navigate('/app', { state: { view: 'state', stateCode: code } });
  };

  return (
    <div className="landing">
      {/* Hero */}
      <section className="landing-hero">
        <div className="landing-container">
          <div className="landing-badge">US Sports Betting Data Intelligence Platform</div>
          <h1 className="landing-headline">
            The fastest sports betting<br />
            data platform on the market.<br />
            <span className="landing-accent">Source-verified.</span>
          </h1>
          <p className="landing-subline">
            Live regulatory data across 35 states. Handle, GGR, market share, and operator
            performance - delivered the moment states publish.
          </p>
          <div className="landing-cta-row">
            <Link to="/app" className="landing-cta">
              Explore Dashboard <ArrowRight size={18} />
            </Link>
            <a href="#contact" className="landing-cta-secondary">Get API Access</a>
          </div>
          <div className="landing-proof">
            <span>35 states</span>
            <span className="landing-proof-dot" />
            <span>40,000+ data points</span>
            <span className="landing-proof-dot" />
            <span className="landing-live-badge">LIVE</span>
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
              <div className="landing-stat-value landing-live-pulse">LIVE</div>
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
              <div
                key={code}
                className="landing-state-chip landing-state-clickable"
                onClick={() => goToState(code)}
              >
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

      {/* API + Contact form */}
      <section className="landing-section" id="contact">
        <div className="landing-container">
          <h2 className="landing-section-title">Get API Access</h2>
          <p className="landing-section-sub">
            Programmatic access to the full dataset. Query by state, operator, date range, or channel.
          </p>
          <div className="landing-code">
            <pre><code>{`curl "https://api.osbdata.com/rest/v1/monthly_data
  ?state_code=eq.NY
  &period_type=eq.monthly
  &order=period_end.desc
  &limit=10"
  -H "apikey: YOUR_API_KEY"`}</code></pre>
          </div>

          <div className="landing-contact-form-wrapper">
            <form className="landing-contact-form" onSubmit={handleSubmit}>
              <div className="landing-form-row">
                <input
                  type="text"
                  placeholder="Name"
                  value={formData.name}
                  onChange={e => setFormData({ ...formData, name: e.target.value })}
                />
                <input
                  type="email"
                  placeholder="Email *"
                  required
                  value={formData.email}
                  onChange={e => setFormData({ ...formData, email: e.target.value })}
                />
              </div>
              <div className="landing-form-row">
                <input
                  type="text"
                  placeholder="Company"
                  value={formData.company}
                  onChange={e => setFormData({ ...formData, company: e.target.value })}
                />
              </div>
              <textarea
                placeholder="Tell us about your use case..."
                rows={3}
                value={formData.message}
                onChange={e => setFormData({ ...formData, message: e.target.value })}
              />
              <button type="submit" className="landing-cta" disabled={formStatus === 'sending'}>
                {formStatus === 'sending' ? 'Sending...' : formStatus === 'sent' ? 'Sent!' : (
                  <>Request Access <Send size={16} /></>
                )}
              </button>
              {formStatus === 'sent' && (
                <p className="landing-form-success">Thanks! We'll be in touch shortly.</p>
              )}
              {formStatus === 'error' && (
                <p className="landing-form-error">Something went wrong. Email us at nosherzapoo@gmail.com</p>
              )}
            </form>
          </div>

          <div style={{ textAlign: 'center', marginTop: 'var(--space-4)' }}>
            <Link to="/app" className="landing-cta-secondary" onClick={() => {
              setTimeout(() => {
                const docsBtn = document.querySelector('button[aria-label="API Docs"]');
                if (docsBtn) docsBtn.click();
              }, 500);
            }}>
              View Full API Documentation
            </Link>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="landing-footer">
        <div className="landing-container">
          <div className="landing-footer-brand">
            <strong>OSB Tracker</strong>
            <span>US Sports Betting Data Intelligence Platform</span>
          </div>
          <div className="landing-footer-links">
            <Link to="/app">Dashboard</Link>
            <a href="#contact">API Access</a>
            <a href="mailto:nosherzapoo@gmail.com">Contact</a>
          </div>
        </div>
      </footer>
    </div>
  );
}

import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { ArrowRight, BarChart3, Shield, Zap, GitCompareArrows, Send, MessageSquare } from 'lucide-react';
import { STATE_NAMES, getOperatorColor } from '../utils/colors';
import { supabase } from '../data/supabase';

// Top 10 states by handle
const TOP_STATES = ['NY','IL','NJ','PA','OH','MI','VA','MA','AZ','NC'];

// Top 6 operators
const TOP_OPERATORS = [
  { name: 'FanDuel', parent: 'Flutter Entertainment' },
  { name: 'DraftKings', parent: 'DraftKings Inc' },
  { name: 'BetMGM', parent: 'Entain / MGM Resorts' },
  { name: 'Caesars', parent: 'Caesars Entertainment' },
  { name: 'Fanatics', parent: 'Fanatics Inc' },
  { name: 'BetRivers', parent: 'Rush Street Interactive' },
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
  const [contactForm, setContactForm] = useState({ name: '', email: '', message: '' });
  const [contactStatus, setContactStatus] = useState(null);
  const [newsletter, setNewsletter] = useState('');
  const [nlStatus, setNlStatus] = useState(null);

  const handleContact = async (e) => {
    e.preventDefault();
    if (!contactForm.email) return;
    setContactStatus('sending');
    try {
      await supabase.from('subscribers').upsert({
        email: contactForm.email,
        name: contactForm.name || null,
        states: '"all"',
        frequency: 'immediate',
        active: true,
      }, { onConflict: 'email' });
      setContactStatus('sent');
      setContactForm({ name: '', email: '', message: '' });
    } catch {
      setContactStatus('error');
    }
  };

  const goToState = (code) => {
    navigate('/app', { state: { view: 'state', stateCode: code } });
  };

  const goToOperator = (name) => {
    navigate('/app', { state: { view: 'operators', operator: name } });
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
            <Link to="/api-access" className="landing-cta-secondary">Request API Access</Link>
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

      {/* Source verification demo */}
      <section className="landing-section">
        <div className="landing-container">
          <h2 className="landing-section-title">Every Number is Source-Verified</h2>
          <p className="landing-section-sub">
            Click any data point and trace it back to the original regulatory filing. No black boxes.
          </p>
          <div className="landing-source-demo">
            <div className="landing-source-mock">
              <div className="landing-source-mock-header">
                <span style={{ font: '500 11px/1 var(--font-display)', textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-tertiary)' }}>Source Verification</span>
              </div>
              <div className="landing-source-mock-body">
                <div className="landing-source-mock-row">
                  <span className="landing-source-mock-label">Value</span>
                  <span style={{ font: '600 28px/1 var(--font-mono)', color: 'var(--text-primary)' }}>$199,232,115</span>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--space-3)' }}>
                  <div>
                    <span className="landing-source-mock-label">Metric</span>
                    <span className="landing-source-mock-value">Handle</span>
                  </div>
                  <div>
                    <span className="landing-source-mock-label">Period</span>
                    <span className="landing-source-mock-value">Mar 22, 2026</span>
                  </div>
                  <div>
                    <span className="landing-source-mock-label">State</span>
                    <span className="landing-source-mock-value">NY - New York</span>
                  </div>
                  <div>
                    <span className="landing-source-mock-label">Operator</span>
                    <span className="landing-source-mock-value">FanDuel</span>
                  </div>
                </div>
                <div style={{ borderTop: '1px solid var(--border-subtle)', paddingTop: 'var(--space-3)', marginTop: 'var(--space-3)' }}>
                  <span className="landing-source-mock-label">Source Document</span>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px', background: 'var(--bg-root)', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', marginTop: 4, font: '400 13px/1.3 var(--font-mono)', color: 'var(--accent-primary)' }}>
                    NY_fanduel_weekly.pdf
                  </div>
                </div>
                <div style={{ marginTop: 'var(--space-3)' }}>
                  <span className="landing-source-mock-label">Raw Source Data</span>
                  <div style={{ padding: '10px 12px', background: 'var(--bg-root)', border: '1px solid var(--border-subtle)', borderLeft: '3px solid var(--accent-secondary)', borderRadius: 'var(--radius-md)', marginTop: 4, font: '400 13px/1.5 var(--font-mono)', color: 'var(--accent-secondary)' }}>
                    03/22/26 $199,232,115 $25,200,002
                  </div>
                </div>
              </div>
            </div>
            <div className="landing-source-text">
              <h3 style={{ font: '600 20px/1.3 var(--font-display)', color: 'var(--text-primary)', margin: '0 0 var(--space-4)' }}>
                Full audit trail on every data point
              </h3>
              <ul style={{ listStyle: 'none', padding: 0, display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
                {[
                  'Direct link to original regulatory PDF',
                  'Raw data line extracted from source',
                  'PDF page screenshot for visual verification',
                  'Scrape timestamp showing when data was collected',
                  'Report URL to access the full filing',
                ].map(item => (
                  <li key={item} style={{ display: 'flex', alignItems: 'flex-start', gap: 8, font: '400 14px/1.5 var(--font-body)', color: 'var(--text-secondary)' }}>
                    <span style={{ color: 'var(--positive)', fontSize: 14, marginTop: 2, flexShrink: 0 }}>&#10003;</span>
                    {item}
                  </li>
                ))}
              </ul>
              <Link to="/app" className="landing-cta" style={{ marginTop: 'var(--space-6)', display: 'inline-flex' }}>
                Try It Live <ArrowRight size={18} />
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* State coverage — top 5 */}
      <section className="landing-section">
        <div className="landing-container">
          <h2 className="landing-section-title">US-Wide Coverage</h2>
          <p className="landing-section-sub">
            Regulatory data from every US state with legal sports betting. Top markets by handle.
          </p>
          <div className="landing-state-grid">
            {TOP_STATES.map(code => (
              <div
                key={code}
                className="landing-state-chip landing-state-clickable"
                onClick={() => goToState(code)}
              >
                <span className="landing-state-code">{code}</span>
                <span className="landing-state-name">{STATE_NAMES[code]}</span>
                <ArrowRight size={12} style={{ marginLeft: 'auto', color: 'var(--text-tertiary)' }} />
              </div>
            ))}
          </div>
          <div style={{ textAlign: 'center', marginTop: 'var(--space-6)' }}>
            <Link to="/app" className="landing-cta-secondary">
              View All 35 States <ArrowRight size={16} />
            </Link>
          </div>
        </div>
      </section>

      {/* Top operators */}
      <section className="landing-section">
        <div className="landing-container">
          <h2 className="landing-section-title">Top Operators</h2>
          <p className="landing-section-sub">
            Track the largest sportsbook operators across every state they operate in.
          </p>
          <div className="landing-operator-grid">
            {TOP_OPERATORS.map(op => (
              <div
                key={op.name}
                className="landing-operator-card"
                onClick={() => goToOperator(op.name)}
              >
                <span className="color-dot" style={{ background: getOperatorColor(op.name), width: 10, height: 10 }} />
                <div>
                  <div className="landing-operator-name">{op.name}</div>
                  <div className="landing-operator-parent">{op.parent}</div>
                </div>
                <ArrowRight size={14} style={{ marginLeft: 'auto', color: 'var(--text-tertiary)' }} />
              </div>
            ))}
          </div>
          <div style={{ textAlign: 'center', marginTop: 'var(--space-6)' }}>
            <Link to="/app" className="landing-cta-secondary" onClick={() => {
              setTimeout(() => {
                const btn = document.querySelector('button[aria-label="Operator View"]');
                if (btn) btn.click();
              }, 500);
            }}>
              View All Operators <ArrowRight size={16} />
            </Link>
          </div>
        </div>
      </section>

      {/* Contact / Help / Suggestions */}
      <section className="landing-section" id="contact">
        <div className="landing-container">
          <h2 className="landing-section-title">Get in Touch</h2>
          <p className="landing-section-sub">
            Questions, feedback, or suggestions? We'd love to hear from you.
          </p>
          <div className="landing-contact-form-wrapper">
            <form className="landing-contact-form" onSubmit={handleContact}>
              <div className="landing-form-row">
                <input
                  type="text"
                  placeholder="Name"
                  value={contactForm.name}
                  onChange={e => setContactForm({ ...contactForm, name: e.target.value })}
                />
                <input
                  type="email"
                  placeholder="Email *"
                  required
                  value={contactForm.email}
                  onChange={e => setContactForm({ ...contactForm, email: e.target.value })}
                />
              </div>
              <textarea
                placeholder="Your message, feedback, or suggestion..."
                rows={4}
                value={contactForm.message}
                onChange={e => setContactForm({ ...contactForm, message: e.target.value })}
              />
              <button type="submit" className="landing-cta" disabled={contactStatus === 'sending'}>
                {contactStatus === 'sending' ? 'Sending...' : contactStatus === 'sent' ? 'Sent!' : (
                  <>Send Message <MessageSquare size={16} /></>
                )}
              </button>
              {contactStatus === 'sent' && (
                <p className="landing-form-success">Thanks for reaching out! We'll get back to you shortly.</p>
              )}
              {contactStatus === 'error' && (
                <p className="landing-form-error">Something went wrong. Email us at moqainvest@gmail.com</p>
              )}
            </form>
          </div>
        </div>
      </section>

      {/* Pricing */}
      <section className="landing-section" id="pricing">
        <div className="landing-container">
          <h2 className="landing-section-title">Pricing</h2>
          <p className="landing-section-sub">
            Free during early access. Paid tiers coming soon.
          </p>
          <div className="landing-pricing-grid">
            <div className="landing-pricing-card">
              <div className="landing-pricing-badge">Current</div>
              <h3 className="landing-pricing-name">Free</h3>
              <div className="landing-pricing-price">$0<span>/month</span></div>
              <ul className="landing-pricing-features">
                <li>National Overview</li>
                <li>Live Feed</li>
                <li>State Deep Dive (35 states)</li>
                <li>Operator Intelligence</li>
                <li>Compare States & Operators</li>
                <li>Source Verification</li>
                <li>Data Export</li>
              </ul>
              <Link to="/app" className="landing-cta" style={{ width: '100%', justifyContent: 'center' }}>
                Open Dashboard
              </Link>
            </div>
            <div className="landing-pricing-card landing-pricing-featured">
              <div className="landing-pricing-badge">Coming Soon</div>
              <h3 className="landing-pricing-name">Pro</h3>
              <div className="landing-pricing-price">$500<span>/month</span></div>
              <ul className="landing-pricing-features">
                <li>Everything in Free</li>
                <li>Email alerts on new data</li>
                <li>Priority data access</li>
                <li>Weekly operator reports</li>
                <li>Dedicated support</li>
              </ul>
              <a href="#newsletter" className="landing-cta-secondary" style={{ width: '100%', justifyContent: 'center', textAlign: 'center' }}>
                Get Early Access
              </a>
            </div>
            <div className="landing-pricing-card">
              <div className="landing-pricing-badge">Coming Soon</div>
              <h3 className="landing-pricing-name">Pro + API</h3>
              <div className="landing-pricing-price">$1,000<span>/month</span></div>
              <ul className="landing-pricing-features">
                <li>Everything in Pro</li>
                <li>REST API access (100K calls/mo)</li>
                <li>Webhook delivery</li>
                <li>Historical data exports</li>
                <li>Custom integrations</li>
              </ul>
              <a href="#newsletter" className="landing-cta-secondary" style={{ width: '100%', justifyContent: 'center', textAlign: 'center' }}>
                Get Early Access
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* Newsletter */}
      <section className="landing-section" id="newsletter">
        <div className="landing-container" style={{ textAlign: 'center' }}>
          <h2 className="landing-section-title">Stay Ahead of the Market</h2>
          <p className="landing-section-sub">
            Get the monthly US Sports Betting Market Report and be first to know when Pro launches.
          </p>
          <form className="landing-newsletter" onSubmit={async (e) => {
            e.preventDefault();
            if (!newsletter) return;
            setNlStatus('sending');
            try {
              await supabase.from('subscribers').upsert({ email: newsletter, states: '"all"', frequency: 'immediate', active: true }, { onConflict: 'email' });
              setNlStatus('done');
              setNewsletter('');
            } catch { setNlStatus('done'); }
          }}>
            <input
              type="email"
              placeholder="your@email.com"
              required
              value={newsletter}
              onChange={e => setNewsletter(e.target.value)}
            />
            <button type="submit" className="landing-cta" disabled={nlStatus === 'sending'}>
              {nlStatus === 'done' ? 'Subscribed!' : 'Subscribe'}
            </button>
          </form>
          {nlStatus === 'done' && (
            <p style={{ color: 'var(--positive)', fontSize: 14, marginTop: 'var(--space-3)' }}>
              You're in! We'll keep you posted.
            </p>
          )}
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
            <Link to="/api-access">API Access</Link>
            <a href="#contact">Contact</a>
          </div>
        </div>
      </footer>
    </div>
  );
}

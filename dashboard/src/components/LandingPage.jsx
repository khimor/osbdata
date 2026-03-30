import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { ArrowRight, BarChart3, Shield, Zap, GitCompareArrows, Send, MessageSquare } from 'lucide-react';
import { STATE_NAMES, getOperatorColor } from '../utils/colors';
import { supabase } from '../data/supabase';

// Top 10 states by handle (hardcoded for fast landing page load)
const TOP_STATES = ['NY','IL','NJ','PA','OH','MI','VA','MA','AZ','NC'];

// Top 5 operators
const TOP_OPERATORS = [
  { name: 'FanDuel', parent: 'Flutter Entertainment' },
  { name: 'DraftKings', parent: 'DraftKings Inc' },
  { name: 'BetMGM', parent: 'Entain / MGM Resorts' },
  { name: 'Caesars', parent: 'Caesars Entertainment' },
  { name: 'Fanatics', parent: 'Fanatics Inc' },
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

      {/* State coverage — top 10 */}
      <section className="landing-section">
        <div className="landing-container">
          <h2 className="landing-section-title">US-Wide Coverage</h2>
          <p className="landing-section-sub">
            Regulatory data from every US state with legal sports betting. Here are the top 10 markets.
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
                <p className="landing-form-error">Something went wrong. Email us at nosherzapoo@gmail.com</p>
              )}
            </form>
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
            <Link to="/api-access">API Access</Link>
            <a href="#contact">Contact</a>
          </div>
        </div>
      </footer>
    </div>
  );
}

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Send, ArrowRight } from 'lucide-react';
import { supabase } from '../data/supabase';

export default function ApiAccessPage() {
  const [form, setForm] = useState({ name: '', email: '', company: '', useCase: '' });
  const [status, setStatus] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.email) return;
    setStatus('sending');
    try {
      // Save full details to contacts table
      await supabase.from('contacts').insert({
        email: form.email,
        name: form.name || null,
        company: form.company || null,
        message: form.useCase || null,
        source: 'api_access',
      });
      // Also add to subscribers for data alerts
      await supabase.from('subscribers').upsert({
        email: form.email,
        name: form.name || null,
        states: '"all"',
        frequency: 'immediate',
        active: true,
      }, { onConflict: 'email' });
      setStatus('sent');
      setForm({ name: '', email: '', company: '', useCase: '' });
    } catch {
      setStatus('error');
    }
  };

  return (
    <div className="landing">
      <section className="landing-hero" style={{ paddingBottom: 40 }}>
        <div className="landing-container">
          <Link to="/" style={{ display: 'inline-flex', alignItems: 'center', gap: 6, color: 'var(--text-secondary)', textDecoration: 'none', fontSize: 14, marginBottom: 'var(--space-6)' }}>
            <ArrowLeft size={16} /> Back to Home
          </Link>
          <h1 className="landing-headline" style={{ fontSize: 40 }}>
            Request <span className="landing-accent">API Access</span>
          </h1>
          <p className="landing-subline">
            Programmatic access to 40,000+ data points across 35 states.
            Query by state, operator, date range, or channel via our REST API.
          </p>
        </div>
      </section>

      <section className="landing-section" style={{ paddingTop: 40 }}>
        <div className="landing-container" style={{ maxWidth: 800 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--space-6)' }}>
            {/* Left: what you get */}
            <div>
              <h3 style={{ font: '600 18px/1.3 var(--font-display)', color: 'var(--text-primary)', marginBottom: 'var(--space-4)' }}>
                What's included
              </h3>
              <ul style={{ listStyle: 'none', padding: 0, display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
                {[
                  'Full REST API access',
                  'All 35 states, 30+ operators',
                  'Handle, GGR, Hold %, Tax data',
                  'Per-operator state breakdowns',
                  'Historical data back to 2018',
                  'Source verification metadata',
                  'Email alerts on new data',
                  'Dedicated support',
                ].map(item => (
                  <li key={item} style={{ display: 'flex', alignItems: 'center', gap: 8, font: '400 14px/1.4 var(--font-body)', color: 'var(--text-secondary)' }}>
                    <span style={{ color: 'var(--positive)', fontSize: 16 }}>&#10003;</span>
                    {item}
                  </li>
                ))}
              </ul>

              <div className="landing-code" style={{ marginTop: 'var(--space-6)' }}>
                <pre><code>{`GET /rest/v1/monthly_data
  ?state_code=eq.NY
  &operator_standard=eq.FanDuel
  &order=period_end.desc
  -H "apikey: YOUR_KEY"`}</code></pre>
              </div>
            </div>

            {/* Right: form */}
            <div>
              <h3 style={{ font: '600 18px/1.3 var(--font-display)', color: 'var(--text-primary)', marginBottom: 'var(--space-4)' }}>
                Tell us about your needs
              </h3>
              <form className="landing-contact-form" onSubmit={handleSubmit}>
                <input
                  type="text"
                  placeholder="Name *"
                  required
                  value={form.name}
                  onChange={e => setForm({ ...form, name: e.target.value })}
                />
                <input
                  type="email"
                  placeholder="Work email *"
                  required
                  value={form.email}
                  onChange={e => setForm({ ...form, email: e.target.value })}
                />
                <input
                  type="text"
                  placeholder="Company"
                  value={form.company}
                  onChange={e => setForm({ ...form, company: e.target.value })}
                />
                <textarea
                  placeholder="How do you plan to use the data? (e.g., internal analytics, client reports, research...)"
                  rows={4}
                  value={form.useCase}
                  onChange={e => setForm({ ...form, useCase: e.target.value })}
                />
                <button type="submit" className="landing-cta" style={{ width: '100%', justifyContent: 'center' }} disabled={status === 'sending'}>
                  {status === 'sending' ? 'Sending...' : status === 'sent' ? 'Request Sent!' : (
                    <>Request Access <Send size={16} /></>
                  )}
                </button>
                {status === 'sent' && (
                  <p className="landing-form-success">Thanks! We'll review your request and get back to you within 24 hours.</p>
                )}
                {status === 'error' && (
                  <p className="landing-form-error">Something went wrong. Email us at khimor@osbdata.com</p>
                )}
              </form>
            </div>
          </div>

          <div style={{ textAlign: 'center', marginTop: 'var(--space-10)' }}>
            <Link to="/app" className="landing-cta-secondary" onClick={() => {
              setTimeout(() => {
                const docsBtn = document.querySelector('button[aria-label="API Docs"]');
                if (docsBtn) docsBtn.click();
              }, 500);
            }}>
              View Full API Documentation <ArrowRight size={16} />
            </Link>
          </div>
        </div>
      </section>

      <footer className="landing-footer">
        <div className="landing-container">
          <div className="landing-footer-brand">
            <strong>OSB Tracker</strong>
            <span>US Sports Betting Data Intelligence Platform</span>
          </div>
          <div className="landing-footer-links">
            <Link to="/">Home</Link>
            <Link to="/app">Dashboard</Link>
            <a href="mailto:khimor@osbdata.com">Contact</a>
          </div>
        </div>
      </footer>
    </div>
  );
}

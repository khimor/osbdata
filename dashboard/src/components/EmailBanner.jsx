import { useState } from 'react';
import { X, Bell } from 'lucide-react';
import { supabase } from '../data/supabase';

const DISMISS_KEY = 'osb_banner_dismissed';
const DISMISS_DAYS = 7;

function isDismissed() {
  const val = localStorage.getItem(DISMISS_KEY);
  if (!val) return false;
  return Date.now() < parseInt(val, 10);
}

export default function EmailBanner() {
  const [dismissed, setDismissed] = useState(isDismissed());
  const [email, setEmail] = useState('');
  const [status, setStatus] = useState(null); // null | 'sending' | 'done'

  if (dismissed) return null;

  const dismiss = () => {
    localStorage.setItem(DISMISS_KEY, String(Date.now() + DISMISS_DAYS * 86400000));
    setDismissed(true);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email) return;
    setStatus('sending');
    try {
      await supabase.from('subscribers').upsert({
        email,
        states: '"all"',
        frequency: 'immediate',
        active: true,
      }, { onConflict: 'email' });
      setStatus('done');
      setTimeout(dismiss, 2000);
    } catch {
      setStatus('done');
      setTimeout(dismiss, 2000);
    }
  };

  return (
    <div className="email-banner">
      <Bell size={14} className="email-banner-icon" />
      {status === 'done' ? (
        <span className="email-banner-text" style={{ color: 'var(--positive)' }}>Subscribed!</span>
      ) : (
        <>
          <span className="email-banner-text">Get notified when new data drops</span>
          <form className="email-banner-form" onSubmit={handleSubmit}>
            <input
              type="email"
              placeholder="your@email.com"
              value={email}
              onChange={e => setEmail(e.target.value)}
              required
            />
            <button type="submit" disabled={status === 'sending'}>
              {status === 'sending' ? '...' : 'Subscribe'}
            </button>
          </form>
        </>
      )}
      <button className="email-banner-close" onClick={dismiss} aria-label="Dismiss">
        <X size={14} />
      </button>
    </div>
  );
}

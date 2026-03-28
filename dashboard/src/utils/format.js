/**
 * Format cents to human-readable currency string.
 * Data is stored in cents (Int64).
 */
export function formatCurrency(cents, opts = {}) {
  if (cents == null || isNaN(cents)) return '-';
  const dollars = Number(cents) / 100;
  const abs = Math.abs(dollars);
  const sign = dollars < 0 ? '-' : '';
  const { compact = true, decimals } = opts;

  if (!compact) {
    return sign + '$' + abs.toLocaleString('en-US', {
      minimumFractionDigits: decimals ?? 2,
      maximumFractionDigits: decimals ?? 2,
    });
  }

  if (abs >= 1_000_000_000) return sign + '$' + (abs / 1_000_000_000).toFixed(2) + 'B';
  if (abs >= 1_000_000) return sign + '$' + (abs / 1_000_000).toFixed(1) + 'M';
  if (abs >= 1_000) return sign + '$' + (abs / 1_000).toFixed(1) + 'K';
  return sign + '$' + abs.toFixed(2);
}

/**
 * Format a raw dollars value (not cents) to human-readable.
 */
export function formatDollars(dollars, opts = {}) {
  if (dollars == null || isNaN(dollars)) return '-';
  return formatCurrency(dollars * 100, opts);
}

/**
 * Format a decimal hold percentage (e.g., 0.085 → "8.50%")
 */
export function formatPct(decimal) {
  if (decimal == null || isNaN(decimal)) return '-';
  return (decimal * 100).toFixed(2) + '%';
}

/**
 * Format a date string for display.
 * "2025-03-31" → "Mar 2025" (monthly) or "Mar 31, 2025" (weekly)
 */
export function formatDate(dateStr, periodType) {
  if (!dateStr) return '-';
  const d = new Date(dateStr + 'T00:00:00');
  if (isNaN(d)) return dateStr;
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  if (periodType === 'weekly') {
    return `${months[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
  }
  return `${months[d.getMonth()]} ${d.getFullYear()}`;
}

/**
 * Format "2025-01" or "2025-01-31" to compact "Jan-25" axis label.
 */
export function formatAxisMonth(str) {
  if (!str) return '';
  const ym = str.slice(0, 7);
  const [y, m] = ym.split('-');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return `${months[parseInt(m, 10) - 1]}-${y.slice(2)}`;
}

/**
 * Compute and format YoY or MoM change.
 */
export function formatChange(current, previous) {
  if (!current || !previous || previous === 0) return null;
  const pct = (current - previous) / Math.abs(previous);
  return {
    pct,
    label: (pct >= 0 ? '+' : '') + (pct * 100).toFixed(1) + '%',
    direction: pct >= 0 ? 'up' : 'down',
  };
}

/**
 * Format large row counts.
 */
export function formatNumber(n) {
  if (n == null) return '-';
  return Number(n).toLocaleString('en-US');
}

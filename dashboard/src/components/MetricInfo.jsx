import { Info } from 'lucide-react';
import { getMetricStatus } from '../data/stateReportingMeta';

/**
 * Shows a "?" info icon when a metric is not reported by a state.
 * Pure CSS tooltip on hover.
 *
 * Props:
 *   stateCode  - e.g. "NJ"
 *   metric     - key from stateReportingMeta: "handle", "standard_ggr", "payouts", "tax_paid"
 *   compact    - smaller icon for table cells (default false)
 *   children   - optional, wraps a value with the icon appended
 */
export default function MetricInfo({ stateCode, metric, compact = false, children }) {
  const status = getMetricStatus(stateCode, metric);

  if (status.reported === true) {
    return children || null;
  }

  const icon = (
    <span className={`metric-info${compact ? ' metric-info--compact' : ''}`}>
      <Info size={compact ? 11 : 13} />
      <span className="metric-info-tooltip">
        {status.reason || `${stateCode} does not report this metric.`}
      </span>
    </span>
  );

  if (children) {
    return (
      <span className="metric-info-wrap">
        {children}
        {icon}
      </span>
    );
  }

  return icon;
}

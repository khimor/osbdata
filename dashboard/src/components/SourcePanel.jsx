import { useMemo } from 'react';
import { X, ExternalLink, Download, FileText } from 'lucide-react';

function SourceContextTable({ contextJson }) {
  const ctx = useMemo(() => {
    if (!contextJson) return null;
    try {
      return typeof contextJson === 'string' ? JSON.parse(contextJson) : contextJson;
    } catch { return null; }
  }, [contextJson]);

  if (!ctx || !ctx.headers || !ctx.rows?.length) return null;

  return (
    <div className="source-context-table-wrapper">
      <table className="source-context-table">
        <thead>
          <tr>
            {ctx.headers.map((h, i) => (
              <th key={i}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {ctx.rows.map((row, ri) => (
            <tr key={ri} className={ri === ctx.highlight ? 'source-context-highlight' : ''}>
              {row.map((cell, ci) => (
                <td key={ci}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function SourcePanel({ data, onClose }) {
  if (!data) return null;

  return (
    <div className="source-overlay" onClick={onClose}>
      <div className="source-panel" onClick={e => e.stopPropagation()}>

        <div className="source-header">
          <span className="source-title">Source verification</span>
          <button className="source-close" onClick={onClose}>
            <X size={16} />
          </button>
        </div>

        <div className="source-body">
          {/* The verified value */}
          <div className="source-section">
            <div className="source-label">Value</div>
            <div className="source-verified-value">{data.formattedValue}</div>
          </div>

          {/* Context */}
          <div className="source-row">
            <div className="source-field">
              <span className="source-label">Metric</span>
              <span className="source-value">{data.metric}</span>
            </div>
            <div className="source-field">
              <span className="source-label">Period</span>
              <span className="source-value">{data.period}</span>
            </div>
          </div>
          <div className="source-row">
            <div className="source-field">
              <span className="source-label">State</span>
              <span className="source-value">{data.stateCode}{data.stateName ? ` - ${data.stateName}` : ''}</span>
            </div>
            <div className="source-field">
              <span className="source-label">Operator</span>
              <span className="source-value">{data.operator || 'All operators'}</span>
            </div>
          </div>

          <div className="source-divider" />

          {/* Source file info */}
          {data.sourceFile && (
            <div className="source-section">
              <div className="source-label">Source document</div>
              {data.sourceUrl ? (
                <a href={data.sourceUrl} target="_blank" rel="noopener noreferrer" className="source-file-link">
                  <FileText size={14} />
                  <span className="source-file-name">{data.sourceFile}</span>
                  <ExternalLink size={12} className="source-file-link-icon" />
                </a>
              ) : (
                <div className="source-file-info">
                  <FileText size={14} />
                  <span>{data.sourceFile}</span>
                </div>
              )}
              {data.sourceUrl && (
                <div className="source-detail source-url-detail" title={data.sourceUrl}>{data.sourceUrl}</div>
              )}
              {data.sourceSheet && (
                <div className="source-detail">Sheet: {data.sourceSheet}</div>
              )}
              {data.sourceRow && (
                <div className="source-detail">Row: {data.sourceRow}</div>
              )}
              {data.sourcePage && (
                <div className="source-detail">Page: {data.sourcePage}</div>
              )}
              {data.sourceColumn && (
                <div className="source-detail">Column: {data.sourceColumn}</div>
              )}
            </div>
          )}

          {/* Raw source line */}
          {data.sourceRawLine && (
            <div className="source-section">
              <div className="source-label">Raw source data</div>
              <pre className="source-raw-line">{data.sourceRawLine}</pre>
            </div>
          )}

          {/* Context table (Excel/CSV/HTML states) */}
          {data.sourceContext && (
            <div className="source-section">
              <div className="source-label">Source data in context</div>
              <SourceContextTable contextJson={data.sourceContext} />
            </div>
          )}

          {/* Links */}
          {data.sourceReportUrl && (
            <div className="source-actions">
              <a href={data.sourceReportUrl} target="_blank" rel="noopener noreferrer" className="source-action-btn">
                <ExternalLink size={14} />
                Open source website
              </a>
            </div>
          )}

          {/* Screenshot */}
          {data.sourceScreenshot && (
            <>
              <div className="source-divider" />
              <div className="source-section">
                <div className="source-label">Source document preview</div>
                <div className="source-screenshot-container">
                  <img
                    src={`/sources/${data.sourceScreenshot}`}
                    alt="Source document screenshot"
                    className="source-screenshot"
                    loading="lazy"
                  />
                </div>
              </div>
            </>
          )}

          {/* Scrape metadata */}
          {data.scrapeTimestamp && (
            <>
              <div className="source-divider" />
              <div className="source-meta">
                Scraped {new Date(data.scrapeTimestamp).toLocaleDateString('en-US', {
                  month: 'short', day: 'numeric', year: 'numeric',
                  hour: '2-digit', minute: '2-digit', timeZoneName: 'short',
                })}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

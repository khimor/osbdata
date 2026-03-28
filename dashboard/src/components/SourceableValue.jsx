import { useState } from 'react';
import SourcePanel from './SourcePanel';
import { formatDate } from '../utils/format';
import { STATE_NAMES } from '../utils/colors';

export default function SourceableValue({ value, formattedValue, row, metric }) {
  const [showSource, setShowSource] = useState(false);

  if (value == null || value === '') return <span>{'-'}</span>;

  const hasProvenance = row && (row.source_file || row.source_url || row.source_report_url);

  const sourceData = hasProvenance ? {
    formattedValue,
    metric,
    period: row.period_end ? formatDate(row.period_end, row.period_type) : '-',
    stateCode: row.state_code,
    stateName: STATE_NAMES[row.state_code] || null,
    operator: row.operator_standard && !['TOTAL', 'ALL'].includes(row.operator_standard)
      ? row.operator_standard : null,
    sourceFile: row.source_file,
    sourceSheet: row.source_sheet,
    sourceRow: row.source_row,
    sourceColumn: row.source_column,
    sourcePage: row.source_page,
    sourceUrl: row.source_url,
    sourceReportUrl: row.source_report_url,
    sourceScreenshot: row.source_screenshot,
    sourceRawLine: row.source_raw_line,
    sourceContext: row.source_context,
    scrapeTimestamp: row.scrape_timestamp,
  } : null;

  if (!hasProvenance) {
    return <span>{formattedValue}</span>;
  }

  return (
    <>
      <span className="sourceable" onClick={() => setShowSource(true)}>
        {formattedValue}
      </span>
      {showSource && (
        <SourcePanel data={sourceData} onClose={() => setShowSource(false)} />
      )}
    </>
  );
}

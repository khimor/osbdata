import { Download } from 'lucide-react';

export default function ExportButton({ data, filename = 'export.csv', columns }) {
  const handleExport = () => {
    if (!data || !data.length) return;

    // Use specified columns or all keys from data
    const cols = columns || Object.keys(data[0]);

    const headers = cols.map(c => typeof c === 'object' ? c.label : c);
    const keys = cols.map(c => typeof c === 'object' ? c.key : c);

    const csvRows = [
      headers.join(','),
      ...data.map(row =>
        keys.map(k => {
          let val = row[k];
          if (val == null) return '';
          const str = String(val);
          if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return '"' + str.replace(/"/g, '""') + '"';
          }
          return str;
        }).join(',')
      ),
    ];

    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename.includes('.') ? filename : `${filename}_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <button className="export-btn" onClick={handleExport} title="Export to CSV">
      <Download />
      <span>CSV</span>
    </button>
  );
}

import clsx from 'clsx';

export function DataTable<T extends Record<string, any>>({ columns, rows }: { columns: { key: keyof T; label: string }[]; rows: T[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="text-left text-slate-500">
            {columns.map(column => (
              <th key={String(column.key)} className="px-3 py-2 font-medium">{column.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx} className={clsx('border-t border-slate-100 dark:border-slate-800', idx % 2 === 0 ? 'bg-white/40 dark:bg-slate-900/40' : '')}>
              {columns.map(column => (
                <td key={String(column.key)} className="px-3 py-2">
                  {String(row[column.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

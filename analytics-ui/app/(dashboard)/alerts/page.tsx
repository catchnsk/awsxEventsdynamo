import { alerts } from '@/lib/mockData';

export default function AlertsPage() {
  const severityColor: Record<string, string> = {
    high: 'bg-red-500/20 text-red-700 dark:text-red-200',
    medium: 'bg-amber-500/20 text-amber-700 dark:text-amber-200',
    low: 'bg-emerald-500/20 text-emerald-700 dark:text-emerald-200'
  };

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Alerts</h2>
      <div className="space-y-3">
        {alerts.map(alert => (
          <div key={alert.name} className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4 flex items-center justify-between">
            <div>
              <p className="text-lg font-medium">{alert.name}</p>
              <p className="text-sm text-slate-500">Status: {alert.status}</p>
            </div>
            <span className={`px-3 py-1 rounded-full text-xs font-semibold ${severityColor[alert.severity] || ''}`}>
              {alert.severity.toUpperCase()}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

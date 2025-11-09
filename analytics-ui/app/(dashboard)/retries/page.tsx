import { retryPipeline } from '@/lib/mockData';

export default function RetriesPage() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Retry & DLQ Pipeline</h2>
      <div className="grid gap-4 md:grid-cols-4">
        {retryPipeline.map(stage => (
          <div key={stage.stage} className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4">
            <p className="text-sm text-slate-500">{stage.stage}</p>
            <p className="text-3xl font-semibold">{stage.count}</p>
          </div>
        ))}
      </div>
      <div className="rounded-xl border border-slate-200 dark:border-slate-800 p-4 bg-white dark:bg-slate-900">
        <p className="text-sm text-slate-500 mb-2">Reprocess Actions</p>
        <p className="text-slate-600 dark:text-slate-300 text-sm">Select DLQ entries to reprocess. RBAC + approval log enforced (stubbed for demo).</p>
      </div>
    </div>
  );
}

import { AreaTrend } from '@/components/AreaTrend';
import { ingestionSeries } from '@/lib/mockData';

export default function IngestionPage() {
  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-semibold">Ingestion Health</h2>
        <p className="text-sm text-slate-500">API throughput, validation errors, and MSK lag</p>
      </header>
      <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4">
        <div className="flex items-center justify-between mb-2">
          <div>
            <p className="text-sm text-slate-500">API Throughput</p>
            <p className="text-xl font-semibold">Avg 780 events/sec</p>
          </div>
          <span className="text-xs uppercase text-slate-500">p95 latency: 82ms</span>
        </div>
        <AreaTrend data={ingestionSeries} />
      </div>
      <div className="grid gap-4 md:grid-cols-2">
        <div className="rounded-xl border border-slate-200 dark:border-slate-800 p-4">
          <h3 className="text-lg font-medium mb-2">Validation Failures</h3>
          <ul className="space-y-2 text-sm text-slate-600 dark:text-slate-300">
            <li>crm.accounts.CustomerUpdated.v3 — 0.3% (missing address)</li>
            <li>billing.invoice.InvoicePaid.v2 — 0.1% (type mismatch)</li>
          </ul>
        </div>
        <div className="rounded-xl border border-slate-200 dark:border-slate-800 p-4">
          <h3 className="text-lg font-medium mb-2">Consumer Lag</h3>
          <p className="text-3xl font-semibold">215</p>
          <p className="text-sm text-slate-500">Max partitions behind on ingress cluster</p>
        </div>
      </div>
    </div>
  );
}

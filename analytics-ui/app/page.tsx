import { KpiCardComponent } from '@/components/KpiCard';
import { AreaTrend } from '@/components/AreaTrend';
import { DataTable } from '@/components/DataTable';
import { overviewKpis, ingestionSeries, partnerPerformance } from '@/lib/mockData';

export default function OverviewPage() {
  return (
    <div className="space-y-6">
      <section>
        <h2 className="text-lg font-semibold mb-3">Today&apos;s KPIs</h2>
        <div className="grid gap-4 grid-cols-1 md:grid-cols-2 xl:grid-cols-4">
          {overviewKpis.map(card => (
            <KpiCardComponent key={card.title} {...card} />
          ))}
        </div>
      </section>

      <section className="grid gap-4 grid-cols-1 xl:grid-cols-3">
        <div className="xl:col-span-2 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4">
          <div className="flex items-center justify-between mb-2">
            <div>
              <p className="text-sm text-slate-500">Ingress RPS</p>
              <p className="text-xl font-semibold">Average {Math.round(ingestionSeries.reduce((acc, item) => acc + item.value, 0) / ingestionSeries.length)} events/sec</p>
            </div>
            <span className="text-sm text-slate-500">Last 24 hours</span>
          </div>
          <AreaTrend data={ingestionSeries} />
        </div>
        <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4">
          <p className="text-sm text-slate-500 mb-2">Top failing partners</p>
          <DataTable
            columns={[
              { key: 'partner', label: 'Partner' },
              { key: 'success', label: 'Success %' },
              { key: 'failure', label: 'Failure %' }
            ]}
            rows={partnerPerformance}
          />
        </div>
      </section>
    </div>
  );
}

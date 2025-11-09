import { partnerPerformance } from '@/lib/mockData';

export default function PartnersPage() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Partner Drill-down</h2>
      <div className="space-y-3">
        {partnerPerformance.map(partner => (
          <div key={partner.partner} className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-lg font-medium">{partner.partner}</p>
                <p className="text-sm text-slate-500">Success {partner.success}% / Failure {partner.failure}%</p>
              </div>
              <button className="text-sm border px-3 py-1 rounded">View timeline</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

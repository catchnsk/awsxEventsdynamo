import { DataTable } from '@/components/DataTable';
import { partnerPerformance } from '@/lib/mockData';

export default function DeliveryPage() {
  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-semibold">Delivery Performance</h2>
        <p className="text-sm text-slate-500">Success/failure rates and latency per partner</p>
      </header>
      <DataTable
        columns={[
          { key: 'partner', label: 'Partner' },
          { key: 'success', label: 'Success %' },
          { key: 'failure', label: 'Failure %' }
        ]}
        rows={partnerPerformance}
      />
    </div>
  );
}

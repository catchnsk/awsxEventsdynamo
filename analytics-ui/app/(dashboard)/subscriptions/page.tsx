import { DataTable } from '@/components/DataTable';
import { subscriptionRows } from '@/lib/mockData';

export default function SubscriptionsPage() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Partner Subscriptions</h2>
      <DataTable
        columns={[
          { key: 'partner', label: 'Partner' },
          { key: 'event', label: 'Event' },
          { key: 'status', label: 'Status' },
          { key: 'deliveryUrl', label: 'Delivery URL' }
        ]}
        rows={subscriptionRows}
      />
    </div>
  );
}

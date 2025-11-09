import { DataTable } from '@/components/DataTable';
import { schemaCatalog } from '@/lib/mockData';

export default function RegistryPage() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Schema Registry</h2>
      <DataTable
        columns={[
          { key: 'domain', label: 'Domain' },
          { key: 'event', label: 'Event' },
          { key: 'version', label: 'Version' },
          { key: 'status', label: 'Status' },
          { key: 'eventsPerDay', label: 'Events/Day' }
        ]}
        rows={schemaCatalog}
      />
    </div>
  );
}

import { DataTable } from '@/components/DataTable';
import { auditLog } from '@/lib/mockData';

export default function AuditPage() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Audit & Governance</h2>
      <DataTable
        columns={[
          { key: 'actor', label: 'Actor' },
          { key: 'action', label: 'Action' },
          { key: 'timestamp', label: 'Timestamp' }
        ]}
        rows={auditLog}
      />
    </div>
  );
}

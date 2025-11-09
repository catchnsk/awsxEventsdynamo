export type KpiCard = {
  title: string;
  value: string;
  delta: string;
  trend: 'up' | 'down' | 'flat';
};

export type ChartPoint = { timestamp: string; value: number };

export const overviewKpis: KpiCard[] = [
  { title: 'Delivery Success', value: '99.2%', delta: '+0.4%', trend: 'up' },
  { title: 'Retry Volume', value: '1,240', delta: '-12%', trend: 'down' },
  { title: 'Avg E2E Latency', value: '1.3s', delta: '+100ms', trend: 'down' },
  { title: 'Schemas Updated', value: '12', delta: '+2', trend: 'up' }
];

export const ingestionSeries: ChartPoint[] = Array.from({ length: 24 }).map((_, idx) => ({
  timestamp: `${idx}:00`,
  value: Math.round(500 + Math.random() * 300)
}));

export const partnerPerformance = [
  { partner: 'Acme CRM', success: 99, failure: 1 },
  { partner: 'PaymentsCo', success: 94, failure: 6 },
  { partner: 'MarketingCloud', success: 97, failure: 3 }
];

export const schemaCatalog = [
  { domain: 'crm.accounts', event: 'CustomerUpdated', version: 'v3', status: 'ACTIVE', eventsPerDay: 12000 },
  { domain: 'billing.invoice', event: 'InvoicePaid', version: 'v2', status: 'ACTIVE', eventsPerDay: 4800 },
  { domain: 'support.tickets', event: 'TicketOpened', version: 'v1', status: 'PENDING', eventsPerDay: 900 }
];

export const subscriptionRows = [
  { partner: 'Acme CRM', event: 'CustomerUpdated', status: 'Approved', deliveryUrl: 'https://hooks.acmecrm.com/customer' },
  { partner: 'PaymentsCo', event: 'InvoicePaid', status: 'Pending', deliveryUrl: 'https://hooks.paymentsco.com/invoice' }
];

export const retryPipeline = [
  { stage: 'Immediate', count: 120 },
  { stage: '5m', count: 60 },
  { stage: '30m', count: 25 },
  { stage: 'DLQ', count: 8 }
];

export const auditLog = [
  { actor: 'alice', action: 'Approved subscription for Acme CRM', timestamp: '2024-05-02T11:00:00Z' },
  { actor: 'bob', action: 'Updated schema billing.invoice v2', timestamp: '2024-05-02T09:30:00Z' }
];

export const alerts = [
  { name: 'Retry queue spike', status: 'firing', severity: 'high' },
  { name: 'Partner down: PaymentsCo', status: 'acknowledged', severity: 'medium' }
];

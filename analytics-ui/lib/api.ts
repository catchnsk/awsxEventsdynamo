import { overviewKpis, ingestionSeries, partnerPerformance, schemaCatalog, subscriptionRows, retryPipeline, auditLog, alerts } from './mockData';

export async function fetchOverview() {
  return { overviewKpis, ingestionSeries, partnerPerformance };
}

export async function fetchRegistry() {
  return schemaCatalog;
}

export async function fetchSubscriptions() {
  return subscriptionRows;
}

export async function fetchDeliveryPerformance() {
  return partnerPerformance;
}

export async function fetchRetries() {
  return retryPipeline;
}

export async function fetchAuditLog() {
  return auditLog;
}

export async function fetchAlerts() {
  return alerts;
}

import { NextResponse } from 'next/server';
import { overviewKpis, ingestionSeries, partnerPerformance, schemaCatalog, subscriptionRows, retryPipeline, auditLog, alerts } from '@/lib/mockData';

export async function GET() {
  return NextResponse.json({
    overviewKpis,
    ingestionSeries,
    partnerPerformance,
    schemaCatalog,
    subscriptionRows,
    retryPipeline,
    auditLog,
    alerts
  });
}

# Analytics UI

Next.js 14 + Tailwind dashboard that implements the requirements in `06-analytics-ui-plan.md`. It ships multiple views (Overview, Ingestion, Schema Registry, Subscriptions, Delivery Performance, Retries/DLQ, Partner Drill-down, Audit, Alerts) and mocks data using static fixtures/API routes.

## Requirements
- Node 20+
- pnpm / npm / yarn (choose one)

## Getting Started
```bash
cd analytics-ui
npm install # or pnpm install
npm run dev
```
Open http://localhost:3000 to view the dashboard.

## Features
- **Overview**: KPI cards, ingestion trend chart, top failing partners.
- **Ingestion Health**: throughput trend, validation failures, consumer lag.
- **Schema Registry & Subscriptions**: searchable tables (stub) with schema/partner metadata.
- **Delivery Performance**: success/failure metrics per partner.
- **Retries & DLQ**: pipeline stage counts and RBAC note for replay workflow.
- **Partner Drill-down**: cards summarizing partner health.
- **Audit & Alerts**: governance entries and alert statuses.

## Structure
```
app/
  layout.tsx          # global nav + header
  page.tsx            # overview
  (dashboard)/*       # feature pages per plan
  api/mock/route.ts   # mock data endpoint
components/           # shared cards/charts/tables
lib/                  # mock data + fetch helpers
```

## Next Steps
- Replace mock data with calls to backend BFF (AppSync/API Gateway).
- Implement RBAC + tenant filter, environment selector, and time-range controls.
- Add React Query provider and secure API integration.
- Hook charts/tables to CloudWatch metrics, DynamoDB tables, and MSK lag data via backend.

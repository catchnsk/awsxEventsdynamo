# Plan: Analytics UI for Webhooks Platform

## 0. Goal
Build a secure, multi-tenant analytics dashboard that visualizes end‑to‑end health of the webhooks platform: schema onboarding, ingestion, processing, dispatch, retries/DLQ, and partner delivery performance.

## 1. Stakeholders & Personas
- **Platform SRE**: cares about pipeline health (lag, errors, saturation).
- **Partner Ops**: watches delivery success by partner, investigates failures.
- **Producer Team**: confirms their event volumes and validation failures.
- **Security/Governance**: audit access and sensitive field handling.
- **Executives**: need high-level KPIs and trend lines.

## 2. Core KPIs
- Ingestion RPS & p95 latency (API + Kafka publish)
- Validation failure rate by event/schema version
- Consumer lag (ingress & egress MSK)
- Dispatch success rate (2xx), failure rate (4xx/5xx), and retry volume
- End‑to‑end latency (producer → partner ACK) p50/p95/p99
- Top N failing partners / endpoints
- DLQ counts & age
- Schema onboarding velocity (new/updated per week)
- Subscription counts by status (Pending/Approved/Cancelled)

## 3. Views / Pages
1. **Overview** (tenant or global)
   - Today’s KPIs, sparkline trends, active incidents.
2. **Ingestion Health**
   - API throughput, validation errors by schema, ingress topic metrics, MSK lag.
3. **Schema Registry**
   - Catalog with search/filter; version lineage; usage (events/day).
4. **Subscriptions**
   - Partner list, delivery URL, status, last webhook, change history.
5. **Delivery Performance**
   - Success/Failure charts by partner, endpoint latency histograms, error taxonomy.
6. **Retries & DLQ**
   - Retry pipeline status, backoff stages, DLQ explorer with RBAC-gated reprocess workflow and approval log.
7. **Partner Drill‑down**
   - Timeline of deliveries, sample payloads (masked), recent errors, circuit‑breaker state.
8. **Audit & Governance**
   - Who changed schemas/subscriptions, when; exportable CSV.
9. **Alerts**
   - Subscribed alarms, thresholds, silence windows (read-only if configured in CloudWatch).

## 4. Tech Stack
- **Frontend**: React + Next.js 15, TypeScript, Tailwind CSS, shadcn/ui, recharts (or Apache ECharts) for charts.
- **API Gateway BFF** (optional): Node.js/Fastify or Spring Boot to aggregate metrics across AWS services.
- **Auth**: Amazon Cognito or Auth0 → OIDC; JWT roles → **RBAC** (admin, operator, viewer, partner).
- **State Mgmt**: React Query for data fetching + caching.
- **Theming/UX**: Light/dark themes, responsive layout, keyboard navigation, a11y AA+.
- **i18n**: react-intl skeleton to support multiple locales later.

## 5. Data Sources & Integration
- **CloudWatch Metrics**: ingestion RPS, Lambda durations, errors, MSK consumer lag.
- **CloudWatch Logs Insights**: ad-hoc queries (via backend) for error sampling.
- **S3 + Athena/Glue**: centralized logs (dispatcher outcomes) → SQL for trend views.
- **DynamoDB**: schema/subscription/audit/status tables via read-only APIs.
- **MSK**: lag via CloudWatch or Kafka JMX exporter metrics.
- **AppConfig**: feature flags (e.g., enable Partner Drill‑down v2).
- **OpenTelemetry**: traces from EKS & Lambda; show waterfall for a sample event.

### Backend/BFF Endpoints (REST)
- `GET /metrics/overview?tenant=` → KPIs
- `GET /metrics/ingress` → API & Kafka metrics
- `GET /registry/schemas?query=&status=`
- `GET /subscriptions?partner=&status=`
- `GET /delivery/performance?partner=&from=&to=`
- `GET /delivery/sample-errors?partner=&event=`
- `GET /retries/summary` and `GET /dlq/items` (with pagination)
- `GET /audit/logs?from=&to=&actor=`

All endpoints perform authorization checks and return **masked** payload samples (tokenized PII).

## 6. Information Architecture & Wireframe Notes
- **Global header**: environment switcher (dev/stage/prod), tenant filter, time range picker.
- **Left nav**: Overview, Ingestion, Registry, Subscriptions, Delivery, Retries/DLQ, Partners, Audit, Alerts.
- **Cards** on Overview: Success %, Avg & p95 E2E latency, Today’s events, Failing partners, DLQ size.
- **Tables**: virtualized for 100k+ rows; column filters, CSV export, deep links.
- **Drill‑down**: clicking a partner opens performance details and recent delivery timeline.

## 7. Security & Privacy
- **RBAC**: admin/operator/viewer; partner role can only see its own subscriptions and deliveries.
- **Row‑level filters** applied in backend, not in the browser.
- **PII**: masked by default; reveal requires explicit scope + reason logging.
- **Signed URLs** for CSV exports; 15‑min expiry.
- **Rate limits** on analytics queries; server‑side caching with ETag.
- **Data retention**: analytics metrics, masked payload samples, and exports are stored for 14 days, after which automated purge jobs remove them.

## 8. Performance
- Query cache (React Query) + server-side caching with `Cache-Control` and revalidation.
- Streaming responses for large tables (chunked transfer).
- Incremental static regeneration for slow‑changing pages (catalogs).

## 9. CI/CD & DevEx
- Monorepo (pnpm/turborepo) or separate repo.
- GitHub Actions: lint, type‑check, unit tests, e2e (Playwright), build, deploy to S3+CloudFront (or Next on ECS/Fargate).
- Infrastructure as code for CloudFront, WAF, Cognito.
- Feature preview environments per PR.

## 10. Telemetry for the UI
- Web Vitals to CloudWatch RUM or Datadog.
- Client events (filter changes, exports) with privacy-safe analytics.
- Error tracking (Sentry/Rollbar).

## 11. Accessibility
- Keyboard nav, focus rings, semantic landmarks.
- Color‑contrast AA+, reduced‑motion mode.
- Screen-reader labels for charts (data table fallback).

## 12. Milestones & Timeline
1. **M0 – Foundations (2–3 wks)**: project setup, auth, layout shell, time range picker, demo data.
2. **M1 – Overview & Ingestion (2 wks)**: KPIs, ingestion charts, MSK lag.
3. **M2 – Registry & Subscriptions (2 wks)**: searchable tables, details panes.
4. **M3 – Delivery Performance (2–3 wks)**: partner drill‑down, latency histograms, error taxonomy.
5. **M4 – Retries/DLQ (1–2 wks)**: retry flows, DLQ explorer, RBAC approval workflow for manual replays.
6. **M5 – Audit & Exports (1 wk)**.
7. **Hardening (ongoing)**: load tests, a11y audits, security review.

## 13. Acceptance Criteria
- p95 UI TTI < 2.5s on 3G; all charts interactive < 250ms updates.
- Every metric and list is filterable by time range and tenant.
- Partner users cannot view other partners' data.
- DLQ replay actions require RBAC approval and emit an audit log entry.
- Exported CSVs are signed & expire; payload samples are masked.

## 14. Nice‑to‑Haves
- Report scheduling to email/Slack.
- Anomaly detection for delivery rates.
- Correlated trace viewer for a single event (ingress → egress → dispatch).

## 15. Open Questions
- _None at this time; partner-facing users will share the main UI/domain with RBAC isolation._

# Plan: AWS Infrastructure for Webhooks Platform

## 0. Scope & Objective
Stand up a multi-AZ, production-grade platform to ingest events, validate/transform, publish to egress topics, and dispatch webhooks with retries and observability. Two MSK clusters are used: **ingress** and **egress**. State is in **DynamoDB** (schema, subscriptions, delivery status). Stateless compute is **Lambda** (Python) and **EKS** (Spring Boot API).

## 1. Accounts, Regions, Naming
- **Accounts**: `dev`, `stage`, `prod` (separate AWS accounts).
- **Primary Region**: `us-east-1` (extensible to a backup region).
- **Naming**: `webhooks-{env}-{component}`; topics: `wh.ingress.{domain}.{event}` / `wh.egress.{partner}.{event}`.

## 2. Networking & Security
- **VPC**: /16 CIDR, 3 public + 3 private subnets across 3 AZs.
- **Endpoints**: VPC endpoints for DynamoDB, S3, CloudWatch Logs, STS, Secrets Manager, ECR, KMS.
- **EKS**: Private API endpoint, managed node groups or Fargate; cluster security groups tightened to MSK/DynamoDB.
- **MSK**: Private subnets only; TLS 1.3; IAM/MSK auth; security groups allow from EKS + Lambda ENIs.
- **API Gateway (private or public as needed)** fronting Lambda admin APIs.
- **KMS**: CMKs for MSK at-rest, DynamoDB SSE, S3, Secrets, and parameter encryption.
- **Secrets Manager/SSM**: store partner credentials, OAuth tokens, webhook secrets.
- **IAM**: least-privilege roles per workload; scoped `dynamodb:*` only to specific tables and keys; `kafka-cluster:*` to specific topics; conditional policies on VPC/private IPs.

## 3. Data Plane Components
### 3.1 Amazon MSK – Kafka
- **Clusters**: `msk-ingress`, `msk-egress` (3 brokers, kafka 3.x, provisioned or serverless per load).
- **Auth**: TLS + IAM authentication (no SASL credentials where possible).
- **Topics**
  - Ingress: `wh.ingress.{domain}.{event}` (with per-tenant partitions; 6+ partitions where throughput requires).
  - Retry: `wh.retry.{event}` (exponential backoff schedule embedded via headers/metadata).
  - DLQ: `wh.dlq.{event}` (non-recoverable events).
  - Egress: `wh.egress.{partner}.{event}`.
- **Configs**: retention (e.g., 3–7 days ingress, 14–30 for retry), compaction for idempotency-ledger topic if used.
- **Observability**: JMX exporter (MSK managed), CloudWatch metrics, broker logs to CloudWatch Logs.

### 3.2 DynamoDB
Tables (PK/SK suggestions):
- **`event_schema`**
  - PK: `PRODUCER_DOMAIN#EVENT_NAME#VERSION`
  - GSIs: `status-index` (ACTIVE/INACTIVE), `contains_sensitive-index`.
- **`partner_event_subscription`**
  - PK: `PARTNER_ID#EVENT_SCHEMA_ID`
  - Attrs: delivery URL, auth, statuses (Pending/Approved/Cancelled).
- **`event_subscription_kafka`**
  - PK: `EVENT_SUBSCRIPTION_ID`
  - Attrs: topic metadata, success/failure counters.
- **`event_delivery_status`**
  - PK: `WEBHOOK_EVENT_UUID`
  - Attrs: event schema id, subscription id, attempt, backoff, partition/offset, final status.
- **`idempotency_ledger`** (optional)
  - PK: `IDEMPOTENCY_KEY`
  - TTL: 24–72h.

All tables: **PITR enabled**, **auto-scaling** RCU/WCU, **SSE-KMS**.

### 3.3 Lambda (Python)
- **Schema Registration Admin**: writes schema into `event_schema` and creates ingress topic.
- **Subscription Admin**: writes `partner_event_subscription` and creates egress topic.
- **Event Processor & Filter**: consumes ingress topics, enriches with partner data, publishes to egress.
- **Webhook Dispatcher**: consumes egress topics, POSTs to partner URL, implements retries + DLQ.
- Packaging: zip layers for shared libs (auth, schema, retry, logging).

### 3.4 EKS (Spring Boot API)
- **Purpose**: REST endpoint for producers (or SDK) to submit events; validates against schema; publishes to ingress topic.
- **Ingress**: NLB/ALB internal; mTLS optional per tenant.
- **Autoscaling**: HPA on CPU/latency; pod disruption budgets.
- **Service Account IAM**: IRSA with `kafka-cluster:*` and `dynamodb:GetItem` permissions.

## 4. Control Plane
- **CI/CD**: GitHub Actions or CodePipeline.
  - Infra: Terraform (preferred) or CDK; one stack per env.
  - App: container build → ECR → deploy to EKS via ArgoCD/GitOps; Lambda via SAM/Serverless/Terraform.
- **Config**: AppConfig for feature flags and partner toggles.
- **Schema Registry**: stored in DynamoDB + versioning; OpenAPI/JSON Schema artifacts in S3.

## 5. Observability
- **Logging**: CloudWatch Logs; structure JSON; correlation ids per event; log retention 30–90 days.
- **Metrics**: CloudWatch + embedded metric format. SLOs: validation success %, delivery success %, p95 end-to-end latency.
- **Tracing**: X-Ray for Lambda; OpenTelemetry for EKS.
- **Alarms**: retries > threshold, DLQ growth, 5xx from partner endpoints, MSK consumer lag, EKS pod restarts.

## 6. Reliability & Backoff
- **Exponential backoff** with jitter; max attempts; circuit breaker per partner.
- **Idempotency**: key derived from event UUID; used in dispatcher; stored in ledger to prevent duplicate delivery.
- **Disaster Recovery**: cross-AZ; snapshots (MSK), PITR (DDB), infra-as-code for rehydrate.

## 7. Costs & Limits
- Use MSK Serverless if unpredictable; otherwise provisioned brokers sized by throughput.
- DynamoDB autoscaling & TTL to manage cost.
- API Gateway and Lambda concurrency limits sized; budgets + alerts.

## 8. Acceptance Checklist
- [ ] Terraform plans applied to `dev`
- [ ] Pen test & threat model complete
- [ ] Chaos tests (broker loss, partner down)

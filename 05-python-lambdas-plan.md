# Plan: Python Lambda Components

All functions use Python 3.11, AWS SDK boto3/botocore v2, and a shared layer `webhooks-common` (logging, metrics, auth, schema utils). Package with SAM/Serverless/Terraform.

## 1. Schema Registration Admin
- **Trigger**: API Gateway `POST /admin/schemas`
- **Actions**:
  - Validate schema document (JSON Schema).
  - Upsert item in `event_schema` (ACTIVE/INACTIVE; versioned).
  - Create/ensure **ingress topic** in MSK (name derived from domain/event/version).
- **IAM**: `dynamodb:PutItem`, `kafka-cluster:CreateTopic`, `kms:Encrypt`.
- **Outputs**: schema ARN/id.

## 2. Subscription Admin
- **Trigger**: API Gateway `POST /admin/subscriptions`
- **Actions**:
  - Validate partner & event references.
  - Upsert to `partner_event_subscription`.
  - Create/ensure **egress topic** for partner/event.
- **IAM**: similar to above.

## 3. Event Processor & Filter
- **Trigger**: MSK consumer (ingress topics) via Lambda event source.
- **Flow**:
  1. Deserialize event envelope.
  2. Optionally transform (mapping table) and enrich by calling Partner Info API.
  3. Publish to **egress topic**; write idempotency marker if provided.
- **Resilience**: batch size 100; partial batch response enabled; retries to retry topic; DLQ on permanent failures.
- **Metrics**: `events_processed`, `events_to_retry`, `events_to_dlq`.

## 4. Webhook Dispatcher
- **Trigger**: MSK consumer (egress topics).
- **Flow**:
  1. Resolve subscription → delivery URL & auth.
  2. POST over HTTPS (TLS 1.3); HMAC signature header optional.
  3. On 2xx: mark `event_delivery_status=SUCCESS`.
  4. On 429/5xx/network: compute next backoff, republish to retry topic.
  5. On permanent failure: write to DLQ, mark FAILED.
- **Idempotency**: check `idempotency_ledger` (if enabled) before POST.
- **Observability**: log delivery latency, partner status codes.

## 5. Shared Concerns
- **Config**: AppConfig or env; timeouts and retry caps per partner.
- **Schema Cache**: read-through cache for validator.
- **Tracing**: X-Ray SDK; correlation id from event envelope.
- **Testing**: pytest + moto/localstack; contract tests for dispatch signatures.

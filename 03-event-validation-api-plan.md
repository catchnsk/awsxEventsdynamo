# Plan: Spring Boot Event Validation API (EKS)

## 0. Purpose
Expose REST endpoints for producers (or SDK) to submit events. Validate against schema registry in DynamoDB and publish to **ingress MSK**.

## 1. Endpoints
- `POST /events/{eventName}`
  - Headers: `X-Event-Version`, `X-Producer-Domain`, `Idempotency-Key` (optional)
  - Body: JSON payload
  - Response: 202 Accepted with `eventId`
- `GET /schemas/{domain}/{event}/{version}` (cached; optional public endpoint)

## 2. Flow
1. Authenticate (JWT/OIDC) and authorize producer.
2. Fetch schema (`event_schema`) and validate payload.
3. Apply transformations/enrichment hooks (if configured).
4. Publish to MSK ingress topic with partition key (tenant or event id).
5. Record idempotency key (optional) and respond 202.

## 3. Tech Stack
- Spring Boot 3.x (Java 17/21), WebFlux.
- AWS SDK v2 (DynamoDB, MSK IAM).
- JSON Schema validator; MapStruct for transforms.
- IRSA for Kafka and DynamoDB permissions.

## 4. Non-Functionals
- p95 < 100ms excluding Kafka acks.
- HPA (CPU/RPS), PDB, liveness/readiness probes.
- OpenAPI 3 + springdoc.
- Structured logging + OTEL tracing.
- Rate limiting (Bucket4j) and request size limits.

## 5. Error Handling
- 400 invalid payload; 409 idempotency conflict; 503 backpressure.
- Kafka publish failures → retry & DLQ/side-channel metric.

## 6. CI/CD
- Dockerfile distroless; ECR.
- Helm chart with config for topic names, auth, and schema cache TTL.

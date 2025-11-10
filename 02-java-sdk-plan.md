# Plan: Webhook Java SDK (Java 21)

## 0. Goals
Provide an ergonomic library for producers to publish events to the platform with validation, auth, retries, and telemetry. Targets Java **21** (current LTS).

## 1. Deliverables
- `webhooks-sdk-core`: validation, signing, idempotency, retry helpers.
- `webhooks-sdk-http`: REST client for EKS API (fallback path).
- `webhooks-sdk-kafka`: direct MSK producer client (optional where IAM auth available).
- BOM + examples; Maven Central / internal artifact repo.

## 2. Public API (fluent)
```java
WebhookClient client = WebhookClient.builder()
    .env("prod")
    .auth(Auth.oidc(clientId, secret))       // or STS/IAM where direct Kafka
    .endpoint("https://events.mycorp")       // used for REST path
    .schemaCache(SchemaCache.inMemory(300))
    .build();

client.publish(Event.of("producer.domain", "CustomerUpdated", "v1")
       .id(UUID.randomUUID().toString())
       .timestamp(Instant.now())
       .payload(jsonNode)
       .headers(Map.of("source","crm")));
```

## 3. Validation
- Pull schema (DynamoDB-backed, surfaced by API) and cache with ETag/version.
- Validate payload via JSON Schema (everit or networknt validator).
- Optional transformation hook before send.

## 4. Transport
- **Primary**: REST to Spring Boot API (EKS) → publishes to **ingress MSK**.
- **Optional**: direct Kafka producer using MSK IAM auth (`software.amazon.msk:aws-msk-iam-auth`); partitioning strategy `{tenantId|eventId}`.

## 5. Security
- OIDC/OAuth2 or JWT access token retrieval (token endpoint configurable).
- mTLS support (keystore/truststore).
- Signing headers (HMAC) for idempotency and auditing.
- PII field tokenization helper (deterministic, AES-GCM with KMS data key).

## 6. Resilience
- Retries with exponential backoff + jitter.
- Local buffer (bounded) for brief outages.
- Circuit breaker when platform returns 429/5xx.

## 7. Telemetry
- SLF4J JSON logs; MDC with `eventId`.
- Micrometer timers/counters; optional OTEL exporter.
- Correlation id propagation.

## 8. Packaging & Build
- Java 21 baseline (still test earlier LTS on demand).
- Gradle Kotlin DSL (or Maven) with reproducible builds, GHA workflow.
- `spotbugs`, `checkstyle`, `errorprone`; Javadoc + unit/integration tests.
- Example app in `/examples/reference-app`.

## 9. Acceptance
- [ ] 90%+ line coverage on validation and signing
- [ ] Load test to 2k events/sec with < 50ms SDK overhead

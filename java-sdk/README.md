# Webhooks Java SDK

This Maven multi-module build implements the SDK described in `02-java-sdk-plan.md`. It ships three artifacts plus a BOM and a reference producer app:

- `webhooks-sdk-core`: fluent client, schema cache, validation, retry helpers, telemetry interfaces.
- `webhooks-sdk-http`: primary REST transport that talks to the Event Validation API.
- `webhooks-sdk-kafka`: optional MSK/Kafka transport using IAM auth.
- `webhooks-sdk-bom`: dependency management for consumers.
- `examples/reference-app`: minimal Spring-style producer using the SDK.

## Requirements
- Java 21+
- Maven 3.9+

## Quickstart
```bash
cd java-sdk
mvn clean install
```

To run the reference app with demo data:
```bash
cd java-sdk/examples/reference-app
mvn exec:java -Dexec.mainClass="com.webhooks.sdk.example.DemoApplication"
```

## Module Notes
- Core module exposes `WebhookClient` configured via builder, supports schema caching/validation and pluggable transports (`WebhookTransport`).
- HTTP module adds `HttpWebhookPublisher` that signs requests, injects idempotency headers, and handles retries.
- Kafka module wraps `KafkaPublisherFactory` that builds MSK IAM-authenticated producers when direct Kafka access is allowed.
- Example app demonstrates wiring the SDK, issuing sample events, and logging responses/end-to-end latency.

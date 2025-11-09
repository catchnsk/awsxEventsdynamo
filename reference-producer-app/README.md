# Reference Producer App

Minimal Spring Boot application that exercises the Webhooks Java SDK (Plan 04). It exposes `POST /demo/customers/{id}` and publishes events through the SDK HTTP transport.

## Prerequisites
1. Build/install the SDK modules locally:
   ```bash
   cd java-sdk
   mvn clean install
   ```
2. Provide a reachable Event Validation API endpoint (or run a mock). For local testing you can point `producer.endpoint` at `http://localhost:8080` and use WireMock.

## Run
```bash
cd reference-producer-app
mvn spring-boot:run
```

Request example:
```bash
curl -X POST http://localhost:8081/demo/customers/123 \
  -H 'Content-Type: application/json' \
  -d '{"status":"ACTIVE","email":"demo@example.com"}'
```

## Configuration
`src/main/resources/application.yaml` holds defaults. Override via env vars (`PRODUCER_DOMAIN`, etc.) or `--producer.domain=...` arguments.

## Local Kafka (optional)
If you want to test end-to-end with MSK-like topics, start the included docker compose file:
```bash
cd reference-producer-app
docker compose up -d
```
Then update `producer.endpoint` to a service that bridges to Kafka (e.g., local Event Validation API) and send test requests.

## Tests
```bash
mvn test
```

Integration tests can be added to hit a local Kafka/validation stack; the included unit test verifies controller wiring with MockMvc.

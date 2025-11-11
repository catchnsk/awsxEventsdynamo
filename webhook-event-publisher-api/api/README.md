# Webhook Event Publisher API

Spring Boot WebFlux service that exposes the endpoints defined in `03-event-validation-api-plan.md`:

- `POST /events/{eventName}` validates payloads against the schema registry (DynamoDB) and publishes to MSK ingress topics.
- `GET /schemas/{domain}/{event}/{version}` returns cached schema metadata for producers/SDKs.

## Requirements
- Java 21+
- Maven 3.9+
- Access to AWS DynamoDB + MSK clusters (or configure local mocks for development)

## Running Locally
```bash
cd webhook-event-publisher-api/api
mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="-Dspring.profiles.active=dev"
```

Environment variables / application properties to set before running against AWS:

| Property | Description |
| --- | --- |
| `webhooks.dynamodb.table-name` | Table storing schemas (`event_schema`) |
| `webhooks.kafka.bootstrap-servers` | MSK broker list or local Kafka endpoint |
| `webhooks.kafka.ingress-topic-prefix` | Topic prefix (e.g., `wh.ingress`) |
| `AWS_REGION` | Region for DynamoDB/MSK |

## Testing
```bash
cd webhook-event-publisher-api/api
mvn test
```

Unit tests use mocked SchemaService/EventPublisher components to keep the feedback loop fast.

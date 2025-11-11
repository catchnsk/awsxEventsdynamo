# Webhook Event Source API

Spring Boot microservice that ingests events from ActiveMQ or Kafka, validates them against schema metadata stored in DynamoDB, optionally transforms them (SOAP/XML/JSON → Avro), and publishes into MSK ingress topics.

## Modules

| Package | Responsibility |
| --- | --- |
| `listener` | ActiveMQ (`AmqListener`) and Kafka (`KafkaSourceListener`) adapters |
| `schema` | DynamoDB-backed schema repository, cache, and validators |
| `transform` | SOAP/XML to JSON converters and JSON → Avro serializer |
| `service` | `EventProcessingService` orchestration, retry/DLQ routing |
| `publisher` | MSK publisher + retry/DLQ topic writers |

## Configuration

`src/main/resources/application.yml` wires the following tree (values are sample defaults):

```yaml
listener:
  source:
    type: amq # switch to kafka for MSK ingress
    amq:
      broker-url: ssl://amq.company.com:61616
      queue: customer.updates
    kafka:
      bootstrap-servers: broker1:9092
      group-id: webhook-event-source-api
      topics:
        - customer_source_topic
  aws:
    region: us-east-1
    dynamo-db:
      table: event_schema
      schema-cache-ttl: 15m
  output:
    msk:
      bootstrap-servers: b-1.msk:9098
      topic-prefix: wh.ingress
      iam-auth-enabled: true
```

Set `listener.source.type=kafka` to enable the Spring Kafka listener, otherwise the JMS listener is used. Schema metadata must include the format (`JSON`, `XML`, `SOAP`, `AVRO`), transform flag (`Y/N`), producer domain, and event name so the MSK topic can be derived as `wh.ingress.<producerDomain>.<eventName>`.

## Running locally

```bash
cd webhook-event-source-api
./mvnw spring-boot:run
```

Provide the broker endpoints, DynamoDB table name, and AWS credentials via environment variables or the default AWS profile.

## Next steps

1. Add Testcontainers-based integration tests for JMS/Kafka flows.
2. Wire AWS IAM/MSK authentication secrets via AWS Secrets Manager or LocalStack for dev.
3. Extend DLQ metadata persistence (e.g., DynamoDB `event_delivery_status`).

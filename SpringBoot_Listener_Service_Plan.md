# Spring Boot Listener Service – Architecture & Implementation Plan

## 0. Purpose
Build a Spring Boot microservice capable of listening to either ActiveMQ or Kafka (configurable), validating incoming messages against schema metadata stored in DynamoDB, optionally transforming the messages (SOAP/XML/JSON → Avro), and publishing the final event to AWS MSK ingress Kafka topics.

---

## 1. High-Level Architecture

```
Source Queues: ActiveMQ / External Kafka
        ↓
Spring Boot Listener Service
   - Listener Layer (AMQ/Kafka)
   - Schema Validator (DynamoDB)
   - Transformation Engine (SOAP/XML/JSON → Avro)
   - Error Handler + Retry + DLQ
        ↓
AWS MSK (Ingress Topics)
```

---

## 2. Features

### ✅ Listen to message sources
- ActiveMQ (JMS)
- Kafka (Spring Kafka)

### ✅ Schema validation
- Retrieves schema metadata from DynamoDB
- Validates JSON/XML/Soap/XML-to-JSON against schema
- Supports Avro schema validation

### ✅ Transformation (optional)
If `transform_flag = "Y"` in DynamoDB:
- SOAP → JSON
- XML → JSON
- JSON → Avro (GenericRecord or SpecificRecord)

### ✅ Publish to AWS MSK
- Uses IAM-based MSK authentication where supported
- Writes into `wh.ingress.<domain>.<event>` topics

---

## 3. DynamoDB Schema Entry Structure

| Field | Description |
|-------|-------------|
| `schema_id` | Unique identifier for schema |
| `schema_definition` | JSON Schema / Avro Schema |
| `format_type` | SOAP / XML / JSON / AVRO |
| `transform_flag` | Y / N |
| `status` | ACTIVE / INACTIVE |
| `producer_domain` | Logical boundary for routing |
| `event_name` | Event name for topic selection |

---

## 4. Configuration Structure

```yaml
source:
  type: amq # amq or kafka
  amq:
    brokerUrl: ssl://amq.company.com:61616
    queue: customer.updates
  kafka:
    bootstrapServers: broker1:9092
    topics:
      - customer_source_topic

aws:
  region: us-east-1
  dynamodb:
    table: event_schema

output:
  msk:
    bootstrapServers: b-1.msk:9098
    topicPrefix: wh.ingress
```

---

## 5. Listener Layer

### ActiveMQ Listener
- Spring JMS
- `DefaultMessageListenerContainer`
- SSL supported
- Recommended for SOAP/XML-heavy legacy systems

### Kafka Listener
- Spring Kafka
- `@KafkaListener` annotated consumer
- Consumer groups for horizontal scaling
- IAM MSK authentication preferred

---

## 6. Schema Validation Layer

### Steps:
1. Lookup schema record using `schema_id` (configured in application.yaml)
2. Cache schema using caffeine TTL=15 min
3. Validate:
   - **JSON** → JSON Schema validation
   - **XML/SOAP** → XSD validation
   - **Avro** → Avro validation

### Validation outcomes
- ✅ Valid → proceed to transformation or direct publish
- ❌ Invalid → Send to DLQ + write error metadata to DynamoDB

---

## 7. Transformation Layer (Optional)

Triggered when:
```
transform_flag = "Y"
```

### Supported conversion flows:
#### SOAP → JSON
- Parse SOAP Envelope → extract Body → convert to structured JSON

#### XML → JSON
- Use Jackson XML mapper

#### JSON → Avro
- Load Avro schema from DynamoDB
- Create `GenericRecord`
- Serialize to Avro binary

---

## 8. Publishing to AWS MSK

Configuration:
```
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Topic Naming:
```
wh.ingress.<producer_domain>.<event_name>
```

Partition Key:
- eventId
- Or correlationId
- Or round-robin fallback

---

## 9. Error Handling & Retry Strategy

### Validation errors
- Send to DLQ: `wh.dlq.validation.<schemaId>`

### Transformation errors
- Log + DLQ
- Write event failure entry to `event_delivery_status` DynamoDB table

### Kafka publish failures
- Retry using exponential backoff
- After max retry → Publish to retry topic `wh.retry.<schemaId>`

---

## 10. Project Structure

```
listener-service/
 ├── listener/
 │     ├── AmqListener.java
 │     ├── KafkaListener.java
 │     └── ListenerConfig.java
 ├── schema/
 │     ├── SchemaRepository.java
 │     ├── DynamoDbSchemaRepository.java
 │     ├── SchemaCache.java
 │     └── SchemaValidator.java
 ├── transform/
 │     ├── SoapToJsonConverter.java
 │     ├── XmlToJsonConverter.java
 │     ├── JsonToAvroConverter.java
 │     └── AvroSchemaLoader.java
 ├── publisher/
 │     ├── MskKafkaProducer.java
 │     └── RetryProducer.java
 └── ListenerServiceApplication.java
```

---

## 11. Non-Functional Requirements

### Performance
- 1000+ messages/sec throughput
- Horizontally scalable consumers

### Reliability
- Built-in DLQ
- Retry logic
- Schema cache fallback

### Security
- MSK IAM auth
- DynamoDB IAM policies (least privilege)
- Secrets via AWS Secrets Manager

### Observability
- Structured JSON logging
- Micrometer → CloudWatch
- Distributed tracing via OpenTelemetry

---

## 12. Delivery Milestones

### Phase 1 — Service Foundation (1 week)
- Spring Boot project setup
- Listener (AMQ/Kafka) configuration
- MSK producer setup

### Phase 2 — Schema Validation (1 week)
- DynamoDB integration
- JSON/XML/SOAP validation

### Phase 3 — Transformation Engine (1–2 weeks)
- SOAP/XML/JSON → Avro transformation
- Avro schema loader

### Phase 4 — DLQ & Retry (1 week)
- DLQ topics
- Retry topics
- Metadata logging

### Phase 5 — Hardening & Testing (1 week)
- Load tests
- Integration tests via Testcontainers
- Documentation & dashboards

---

## 13. Deliverables

- Source code for Listener Service
- Integration tests (AMQ/Kafka → MSK)
- Avro schema examples
- DynamoDB schema examples
- Deployment Helm chart or Terraform modules
- Monitoring dashboards & alerts

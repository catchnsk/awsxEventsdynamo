# Event Publishing Logic Documentation

This document describes the logic flow for all event publishing endpoints in the Webhook Event Publisher API.

## Table of Contents

1. [POST /webhook/event/publisherCE - CloudEvents Format](#1-post-webhookeventpublisherce---cloudevents-format)
2. [POST /webhook/event/publisher - Generic Format](#2-post-webhookeventpublisher---generic-format)
3. [POST /webhook/schema/schema_id/{schemaId} - By Schema ID](#3-post-webhookschemaschema_idschemaid---by-schema-id)
4. [Common Logic](#common-logic)
5. [Error Handling](#error-handling)

---

## 1. POST /webhook/event/publisherCE - CloudEvents Format

**Method:** `publishCloudEvent()`  
**Controller:** `EventController`  
**Interface:** `DefaultApi`

### Flow Diagram

```
Request (CloudEvent)
    ↓
1. Extract Event ID & Idempotency Key
    ├─ Use X-Event-Id header OR event.id from payload
    └─ Use Idempotency-Key header OR eventId as fallback
    ↓
2. Validate Mandatory CloudEvents Fields
    ├─ source (required) → Maps to PRODUCER_DOMAIN
    ├─ subject (required) → Maps to EVENT_NAME
    ├─ specVersion (required) → Maps to VERSION
    └─ data (required) → Payload to validate
    ↓
3. Schema Lookup
    ├─ Create SchemaReference(source, subject, specVersion)
    ├─ Fetch schema from DynamoDB (with caching)
    └─ Error if schema not found (404)
    ↓
4. Schema Validation Checks (if validation enabled)
    ├─ Check JSON Schema exists in EVENT_SCHEMA_DEFINITION
    ├─ Verify it's not an Avro schema (starts with {"type":"record"})
    └─ Error if invalid (400)
    ↓
5. Data Validation (if validation enabled)
    ├─ Convert event.data to JsonNode
    ├─ Validate against JSON Schema
    └─ Error if validation fails (400)
    ↓
6. Topic Name Construction
    └─ Format: {kafka.ingressTopicPrefix}.{domain}.{eventName}
       Example: "wh.ingress.payments.transactionCreated"
    ↓
7. Create EventEnvelope
    ├─ eventId
    ├─ SchemaReference
    ├─ Metadata (type, source, subject, specVersion, time)
    ├─ Timestamp
    ├─ Headers (Idempotency-Key, Event-Type, Source)
    └─ Format: JSON_SCHEMA
    ↓
8. Publish to Kafka
    ├─ Publish as JSON string
    └─ Returns published event ID
    ↓
9. Record in Idempotency Ledger
    ├─ EVENT_ID: publishedEventId
    ├─ EVENT_STATUS: EVENT_READY_FOR_DELIVERY
    └─ EVENT_SCHEMAID: SCHEMA_{DOMAIN}_{EVENT}_{VERSION}
    ↓
10. Return Response
    └─ 202 Accepted with AckResponse containing eventId
```

### Detailed Steps

#### Step 1: Extract Event ID & Idempotency Key
```java
String eventId = xEventId != null ? xEventId.toString() : event.getId().toString();
String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
```

#### Step 2: Validate Mandatory Fields
- **source**: Required, maps to `PRODUCER_DOMAIN` in DynamoDB
- **subject**: Required, maps to `EVENT_NAME` in DynamoDB
- **specVersion**: Required, maps to `VERSION` in DynamoDB
- **data**: Required, contains the event payload

#### Step 3: Schema Lookup
```java
SchemaReference reference = new SchemaReference(
    event.getSource(),      // source → PRODUCER_DOMAIN
    event.getSubject(),     // subject → EVENT_NAME
    event.getSpecVersion()  // specVersion → VERSION
);
schemaService.fetchSchema(reference)
```

#### Step 4: Schema Validation Checks (if validation enabled)
- Checks if `EVENT_SCHEMA_DEFINITION` exists
- Verifies it's not an Avro schema (doesn't start with `{"type":"record"}`)
- Returns 400 if validation fails

#### Step 5: Data Validation (if validation enabled)
```java
JsonNode dataJsonNode = objectMapper.valueToTree(event.getData());
jsonSchemaValidator.validate(dataJsonNode, schemaDefinition)
```

#### Step 6: Topic Name Construction
```java
String topicName = "%s.%s.%s".formatted(
    properties.kafka().ingressTopicPrefix(),  // e.g., "wh.ingress"
    reference.domain(),                        // e.g., "payments"
    reference.eventName()                      // e.g., "transactionCreated"
);
// Result: "wh.ingress.payments.transactionCreated"
```

#### Step 7: Create EventEnvelope
```java
EventEnvelope envelope = new EventEnvelope(
    eventId,
    reference,
    metadata,  // Contains: type, source, subject, specVersion, time
    timestamp,
    headers,   // Contains: Idempotency-Key, Event-Type, Source
    SchemaFormatType.JSON_SCHEMA
);
```

#### Step 8: Publish to Kafka
```java
eventPublisher.publishJson(envelope, topicName, validatedJson)
```

#### Step 9: Record in Idempotency Ledger
```java
idempotencyLedgerService.recordEventStatus(
    publishedEventId,
    EventStatus.EVENT_READY_FOR_DELIVERY,
    schemaId  // Format: SCHEMA_{DOMAIN}_{EVENT}_{VERSION}
)
```

### Special Features

- ✅ **Validation Toggle**: Can be disabled via `webhooks.validation.enabled: false`
- ✅ **CloudEvents ID**: Uses CloudEvents `id` field as event ID
- ✅ **Field Mapping**: Maps CloudEvents fields to DynamoDB schema lookup
- ✅ **Idempotency Tracking**: Records status in `EVENT_IDEMPOTENCY_LEDGER` table
- ✅ **Error Tracking**: Records processing/delivery failures in ledger

### Request Example
```json
{
  "id": "F701560E-A31B-4748-927F-2655CAF785F9",
  "type": "com.bee.us.card.activation",
  "source": "payments",
  "subject": "transactionCreated",
  "specVersion": "1.0",
  "time": "2025-11-13T23:00:00Z",
  "data": {
    "transactionId": "txn-999",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS"
  }
}
```

### Response Example
```json
{
  "eventId": "F701560E-A31B-4748-927F-2655CAF785F9"
}
```

---

## 2. POST /webhook/event/publisher - Generic Format

**Method:** `publishEvent()`  
**Controller:** `EventController`  
**Interface:** `DefaultApi`

### Flow Diagram

```
Request (InlineObject with domain, eventName, version, data)
    ↓
1. Extract Event ID & Idempotency Key
    ├─ Use X-Event-Id header OR generate UUID
    └─ Use Idempotency-Key header OR eventId as fallback
    ↓
2. Parse Request Body
    ├─ Extract domain, eventName, version, data from InlineObject
    └─ Convert data to JSON string
    ↓
3. Schema Lookup
    ├─ Create SchemaReference(domain, eventName, version)
    ├─ Fetch schema from DynamoDB (with caching)
    └─ Error if schema not found (404)
    ↓
4. Format Detection
    ├─ Determine Content-Type (JSON/XML/Avro)
    └─ Convert payload to JsonNode
    ↓
5. Schema Format Branching
    ├─ If JSON_SCHEMA format:
    │   ├─ Validate against JSON Schema (if enabled)
    │   ├─ Publish as JSON string
    │   └─ Topic: {prefix}.{domain}.{eventName}
    │
    └─ If AVRO_SCHEMA format:
        ├─ Coerce JSON types to Avro types
        ├─ Validate against Avro Schema (if enabled)
        ├─ Serialize to Avro binary
        └─ Publish as Avro bytes
    ↓
6. Topic Name Construction
    └─ Format: {kafka.ingressTopicPrefix}.{domain}.{eventName}
    ↓
7. Create EventEnvelope
    ├─ eventId
    ├─ SchemaReference
    ├─ Metadata (originalFormat)
    ├─ Timestamp
    ├─ Headers (Idempotency-Key, Original-Format)
    └─ Format: JSON_SCHEMA or AVRO_SCHEMA
    ↓
8. Publish to Kafka
    ├─ JSON: publishJson(envelope, topicName, validatedJson)
    └─ Avro: publishAvro(envelope, topicName, avroBytes)
    ↓
9. Return Response
    └─ 202 Accepted with AckResponse containing eventId
```

### Detailed Steps

#### Step 1: Extract Event ID & Idempotency Key
```java
String eventId = xEventId != null ? xEventId.toString() : UUID.randomUUID().toString();
String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
```

#### Step 2: Parse Request Body
```java
String domain = body.getDomain();
String eventName = body.getEventName();
String version = body.getVersion();
Map<String, Object> data = body.getData();
```

#### Step 3: Schema Lookup
```java
SchemaReference reference = new SchemaReference(domain, eventName, version);
schemaService.fetchSchema(reference)
```

#### Step 4: Format Detection
```java
SchemaFormat format = SchemaFormat.fromContentType(contentType);
// Supports: application/json, application/xml, application/avro
convertToJson(payload, format)  // Converts XML/Avro to JsonNode
```

#### Step 5: Schema Format Branching

**JSON Schema Path:**
```java
if (schemaDefinition.formatType() == SchemaFormatType.JSON_SCHEMA) {
    // Validate (if enabled)
    jsonSchemaValidator.validate(jsonNode, schemaDefinition)
    // Publish as JSON
    eventPublisher.publishJson(envelope, topicName, validatedJson)
}
```

**Avro Schema Path:**
```java
else {
    // Coerce types (important for XML where all values are strings)
    JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);
    // Validate (if enabled)
    avroSchemaValidator.validate(coercedJson, schemaDefinition)
    // Serialize to Avro binary
    avroSerializer.serializeToAvro(avroRecord, avroSchema)
    // Publish as Avro bytes
    eventPublisher.publishAvro(envelope, topicName, avroBytes)
}
```

### Special Features

- ✅ **Multi-Format Support**: Accepts JSON, XML, and Avro input formats
- ✅ **Auto-Detection**: Automatically detects schema format (JSON_SCHEMA vs AVRO_SCHEMA)
- ✅ **Type Coercion**: Handles type coercion for Avro (important for XML input)
- ✅ **Validation Toggle**: Can be disabled via `webhooks.validation.enabled: false`

### Request Example
```json
{
  "domain": "payments",
  "eventName": "transactionCreated",
  "version": "1.0",
  "data": {
    "transactionId": "txn-999",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS"
  }
}
```

### Response Example
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## 3. POST /webhook/schema/schema_id/{schemaId} - By Schema ID

**Method:** `publishEventBySchemaId()`  
**Controller:** `EventController`  
**Interface:** `DefaultApi`

### Flow Diagram

```
Request (schemaId + payload)
    ↓
1. Extract Event ID & Idempotency Key
    ├─ Use X-Event-Id header OR generate UUID
    └─ Use Idempotency-Key header OR eventId as fallback
    ↓
2. Parse Request Body
    ├─ Convert Object to Map<String, Object>
    └─ Convert to JSON string
    ↓
3. Schema Lookup by Schema ID
    ├─ Fetch schema by EVENT_SCHEMA_ID from DynamoDB
    └─ Error if schema not found (404)
    ↓
4. Schema Status Validation
    ├─ Check EVENT_SCHEMA_STATUS == "ACTIVE"
    ├─ Check TOPIC_STATUS == "ACTIVE"
    └─ Check TOPIC_NAME exists
    └─ Error if any check fails (400)
    ↓
5. Schema Definition Validation
    ├─ Check at least one schema exists (JSON or Avro)
    └─ Error if missing (400)
    ↓
6. Format Detection & Conversion
    ├─ Determine Content-Type (JSON/XML/Avro)
    └─ Convert payload to JsonNode
    ↓
7. Schema Format Branching
    ├─ If JSON_SCHEMA format:
    │   ├─ Validate against JSON Schema (if enabled)
    │   ├─ Publish as JSON string
    │   └─ Use topicName from schema
    │
    └─ If AVRO_SCHEMA format:
        ├─ Coerce JSON types to Avro types
        ├─ Validate against Avro Schema (if enabled)
        ├─ Serialize to Avro binary
        └─ Use topicName from schema
    ↓
8. Create EventEnvelope
    ├─ eventId
    ├─ SchemaReference (from schema)
    ├─ Metadata (schemaId)
    ├─ Timestamp
    ├─ Headers (Idempotency-Key, Original-Format, Schema-Id)
    └─ Format: JSON_SCHEMA or AVRO_SCHEMA
    ↓
9. Publish to Kafka
    ├─ JSON: publishJson(envelope, topicName, validatedJson)
    └─ Avro: publishAvro(envelope, topicName, avroBytes)
    ↓
10. Return Response
    └─ 202 Accepted with AckResponse containing eventId
```

### Detailed Steps

#### Step 1: Extract Event ID & Idempotency Key
```java
String eventId = xEventId != null ? xEventId.toString() : UUID.randomUUID().toString();
String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
```

#### Step 2: Parse Request Body
```java
Map<String, Object> bodyMap;
if (body instanceof Map) {
    bodyMap = (Map<String, Object>) body;
} else {
    bodyMap = objectMapper.convertValue(body, Map.class);
}
```

#### Step 3: Schema Lookup by Schema ID
```java
schemaService.fetchSchemaBySchemaId(schemaId)
// Returns SchemaDetailResponse with full schema details
```

#### Step 4: Schema Status Validation
```java
// Validate schema status
if (!"ACTIVE".equals(schemaDetail.eventSchemaStatus())) {
    return Mono.error(new ResponseStatusException(BAD_REQUEST, "Schema is not ACTIVE"));
}

// Validate topic status
if (!"ACTIVE".equals(schemaDetail.topicStatus())) {
    return Mono.error(new ResponseStatusException(BAD_REQUEST, "Topic is not ACTIVE"));
}

// Validate topic name exists
if (schemaDetail.topicName() == null || schemaDetail.topicName().isEmpty()) {
    return Mono.error(new ResponseStatusException(BAD_REQUEST, "Topic name not configured for schema"));
}
```

#### Step 5: Schema Definition Validation
```java
boolean hasJsonSchema = schemaDefinition.jsonSchema() != null && !schemaDefinition.jsonSchema().isEmpty();
boolean hasAvroSchema = schemaDefinition.avroSchema() != null && !schemaDefinition.avroSchema().isEmpty();

if (!hasJsonSchema && !hasAvroSchema) {
    return Mono.error(new ResponseStatusException(BAD_REQUEST, "Schema definition not configured for this event"));
}
```

#### Step 6: Format Detection & Conversion
```java
SchemaFormat format = SchemaFormat.fromContentType(contentType);
convertToJson(payload, format)  // Converts XML/Avro to JsonNode
```

#### Step 7: Schema Format Branching

**JSON Schema Path:**
```java
if (schemaDefinition.formatType() == SchemaFormatType.JSON_SCHEMA) {
    handleJsonSchemaValidationBySchemaId(
        jsonNode, schemaDefinition, reference,
        eventId, idempotencyKeyValue, format,
        schemaDetail.topicName(), schemaId
    )
}
```

**Avro Schema Path:**
```java
else {
    handleAvroSchemaValidationBySchemaId(
        jsonNode, schemaDefinition, reference,
        eventId, idempotencyKeyValue, format,
        schemaDetail.topicName(), schemaId,
        schemaDefinition.avroSchema()
    )
}
```

#### Step 8: Create EventEnvelope
```java
EventEnvelope envelope = new EventEnvelope(
    eventId,
    reference,  // From schema: producerDomain, eventName, version
    objectMapper.valueToTree(Map.of("schemaId", schemaId)),
    Instant.now(),
    Map.of(
        "Idempotency-Key", idempotencyKey,
        "Original-Format", format.name(),
        "Schema-Id", schemaId
    ),
    SchemaFormatType.JSON_SCHEMA or AVRO_SCHEMA
);
```

#### Step 9: Publish to Kafka
- Uses `topicName` from DynamoDB schema record (not constructed)
- JSON: `eventPublisher.publishJson(envelope, topicName, validatedJson)`
- Avro: `eventPublisher.publishAvro(envelope, topicName, avroBytes)`

### Special Features

- ✅ **Direct Schema Lookup**: Uses `EVENT_SCHEMA_ID` for direct schema lookup
- ✅ **Status Validation**: Validates schema and topic status (must be ACTIVE)
- ✅ **Topic from Schema**: Uses `topicName` from DynamoDB schema record
- ✅ **Multi-Format Support**: Supports JSON, XML, and Avro input formats
- ✅ **Validation Toggle**: Can be disabled via `webhooks.validation.enabled: false`

### Request Example
```json
{
  "transactionId": "txn-999",
  "customerId": "cust-001",
  "amount": 150.75,
  "currency": "USD",
  "status": "SUCCESS"
}
```

**Path Parameter:** `schemaId=SCHEMA_0001`

### Response Example
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## Common Logic

### Validation Flow (if enabled)

```
1. Check webhooks.validation.enabled flag
   ├─ If false → Skip validation, proceed to publish
   └─ If true → Continue with validation

2. JSON Schema Validation:
   ├─ Parse JSON Schema from EVENT_SCHEMA_DEFINITION
   ├─ Validate payload against schema
   └─ Return detailed error messages on failure

3. Avro Schema Validation:
   ├─ Parse Avro Schema from EVENT_SCHEMA_DEFINITION_AVRO
   ├─ Coerce JSON types to match Avro types
   ├─ Validate payload against schema
   └─ Return detailed error messages on failure
```

### Validation Toggle

All endpoints respect the `webhooks.validation.enabled` configuration flag:

- **Enabled (default)**: Full schema validation is performed
- **Disabled**: Validation is skipped, but events are still published to Kafka

**Configuration:**
```yaml
webhooks:
  validation:
    enabled: true  # Set to false to skip validation (testing only)
```

### Schema Format Detection

The system automatically detects which schema format to use:

1. **JSON_SCHEMA**: If `EVENT_SCHEMA_DEFINITION` exists and is not an Avro schema
2. **AVRO_SCHEMA**: If only `EVENT_SCHEMA_DEFINITION_AVRO` exists, or if `EVENT_SCHEMA_DEFINITION` contains an Avro schema

### Type Coercion (for Avro)

When processing Avro schemas, the system performs type coercion:
- XML input: All values are strings, coerced to appropriate Avro types
- JSON input: Types are coerced to match Avro schema requirements
- Handles: INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, RECORD, ARRAY, MAP, UNION

### Topic Name Construction

**For `/webhook/event/publisherCE` and `/webhook/event/publisher`:**
```java
String topicName = "%s.%s.%s".formatted(
    properties.kafka().ingressTopicPrefix(),  // e.g., "wh.ingress"
    domain,                                    // e.g., "payments"
    eventName                                  // e.g., "transactionCreated"
);
// Result: "wh.ingress.payments.transactionCreated"
```

**For `/webhook/schema/schema_id/{schemaId}`:**
- Uses `topicName` directly from DynamoDB schema record
- No construction needed

### EventEnvelope Structure

```java
EventEnvelope(
    String eventId,                    // Unique event identifier
    SchemaReference reference,         // domain, eventName, version
    JsonNode metadata,                  // Additional metadata
    Instant timestamp,                  // Event timestamp
    Map<String, String> headers,        // Kafka headers
    SchemaFormatType formatType        // JSON_SCHEMA or AVRO_SCHEMA
)
```

### Kafka Publishing

**JSON Format:**
```java
eventPublisher.publishJson(envelope, topicName, jsonNode)
// Publishes as JSON string
```

**Avro Format:**
```java
eventPublisher.publishAvro(envelope, topicName, avroBytes)
// Publishes as Avro binary
```

### Idempotency Ledger (for `/webhook/event/publisherCE` only)

Records event status in `EVENT_IDEMPOTENCY_LEDGER` DynamoDB table:

**Table Structure:**
- `EVENT_ID` (PK): String - Event identifier
- `EVENT_STATUS` (SK): String - Status value
- `CREATION_TIMESTAMP`: String - ISO 8601 timestamp
- `UPDATED_TIMESTAMP`: String - ISO 8601 timestamp
- `EVENT_SCHEMAID`: String - Schema identifier

**Status Values:**
- `EVENT_READY_FOR_PROCESSING`
- `EVENT_READY_FOR_DELIVERY`
- `EVENT_DELIVERED`
- `EVENT_PROCESSING_FAILED`
- `EVENT_DELIVERY_FAILED`

**Recording Logic:**
```java
// On successful publish
idempotencyLedgerService.recordEventStatus(
    publishedEventId,
    EventStatus.EVENT_READY_FOR_DELIVERY,
    schemaId
)

// On processing failure
idempotencyLedgerService.recordEventStatus(
    eventId,
    EventStatus.EVENT_PROCESSING_FAILED,
    null
)

// On delivery failure
idempotencyLedgerService.recordEventStatus(
    eventId,
    EventStatus.EVENT_DELIVERY_FAILED,
    null
)
```

---

## Error Handling

### HTTP Status Codes

| Status Code | Description | When It Occurs |
|-------------|-------------|----------------|
| **202 Accepted** | Event accepted and published | Success |
| **400 Bad Request** | Validation failed, missing fields, inactive schema/topic | Validation errors, missing required fields, schema/topic not active |
| **404 Not Found** | Schema not found | Schema doesn't exist in DynamoDB |
| **500 Internal Server Error** | DynamoDB table not found | DynamoDB table doesn't exist |
| **503 Service Unavailable** | Kafka or DynamoDB unavailable | Service connection issues |

### Error Response Format

```json
{
  "timestamp": "2025-12-05T22:00:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed: ...",
  "path": "/webhook/event/publisherCE"
}
```

### Common Error Scenarios

#### 1. Validation Errors
- **JSON Schema validation failed**: Payload doesn't match schema
- **Avro Schema validation failed**: Payload doesn't match Avro schema
- **Type mismatch**: Data type doesn't match schema definition

#### 2. Schema Errors
- **Schema not found**: No schema exists for the given criteria
- **Schema not configured**: `EVENT_SCHEMA_DEFINITION` or `EVENT_SCHEMA_DEFINITION_AVRO` is missing
- **Schema is not ACTIVE**: Schema status is INACTIVE
- **Topic is not ACTIVE**: Topic status is INACTIVE
- **Topic name not configured**: Topic name is missing from schema

#### 3. Service Errors
- **DynamoDB table not found**: Table doesn't exist
- **DynamoDB service unavailable**: Connection issues
- **Kafka is unavailable**: Kafka broker connection issues

### Error Logging

All errors are logged with detailed information:
- Request details (source, subject, specVersion, etc.)
- Schema information
- Validation errors with field paths
- Stack traces for debugging

---

## Configuration

### Application Configuration

```yaml
webhooks:
  dynamodb:
    table-name: event_schema
    idempotency-ledger-table-name: EVENT_IDEMPOTENCY_LEDGER
  kafka:
    bootstrap-servers: localhost:9092
    ingress-topic-prefix: wh.ingress
  cache:
    enabled: true
    schema-ttl: PT24H
  validation:
    enabled: true  # Set to false to skip validation (testing only)
```

### Environment Variables

- `aws.dynamodb.endpoint`: DynamoDB endpoint (for local: `http://localhost:8000`)
- `aws.region`: AWS region (default: `us-east-1`)

---

## Summary

### Endpoint Comparison

| Feature | `/webhook/event/publisherCE` | `/webhook/event/publisher` | `/webhook/schema/schema_id/{schemaId}` |
|---------|----------------------------|---------------------------|----------------------------------------|
| **Input Format** | CloudEvents | Generic (domain/event/version) | Generic payload |
| **Schema Lookup** | source/subject/specVersion | domain/eventName/version | EVENT_SCHEMA_ID |
| **Schema Format** | JSON Schema only | JSON or Avro | JSON or Avro |
| **Topic Name** | Constructed | Constructed | From schema |
| **Status Validation** | No | No | Yes (ACTIVE checks) |
| **Idempotency Ledger** | Yes | No | No |
| **Input Formats** | JSON only | JSON/XML/Avro | JSON/XML/Avro |

### Key Differences

1. **`/webhook/event/publisherCE`**:
   - CloudEvents format only
   - Maps CloudEvents fields to schema lookup
   - Records in idempotency ledger
   - JSON Schema validation only

2. **`/webhook/event/publisher`**:
   - Generic format with domain/event/version in body
   - Supports JSON, XML, and Avro input
   - Auto-detects schema format (JSON or Avro)
   - Constructs topic name

3. **`/webhook/schema/schema_id/{schemaId}`**:
   - Direct schema lookup by ID
   - Validates schema and topic status
   - Uses topic name from schema
   - Supports JSON, XML, and Avro input

---

## Notes

- All endpoints support validation toggle via `webhooks.validation.enabled`
- Schema caching is enabled by default (24-hour TTL)
- All endpoints use reactive programming (Mono/Flux)
- Error handling includes detailed logging for debugging
- Idempotency ledger is only used for CloudEvents endpoint


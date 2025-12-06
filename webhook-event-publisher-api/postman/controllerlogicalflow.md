Event Publishing Logic
1. POST /webhook/event/publisherCE - CloudEvents Format
   Method: publishCloudEvent()
   Flow:
1. Extract Event ID & Idempotency Key   
        ├ ─ Use X-Event-Id header OR event.id from payload   
        └─ Use Idempotency-Key header OR eventId as fallback2. Validate Mandatory CloudEvents Fields   
   2. ├─ source (required) → Maps to PRODUCER_DOMAIN  
   3. ├─ subject (required) → Maps to EVENT_NAME  
   4. ├─ specVersion (required) → Maps to VERSION  
   5. └─ data (required) → Payload to validate3. Schema Lookup   
   6. ├─ Create SchemaReference(source, subject, specVersion)  
   7. ├─ Fetch schema from DynamoDB (with caching)   
   8. └─ Error if schema not found (404)4. Schema Validation Checks (if validation enabled)  
   9. ├─ Check JSON Schema exists in EVENT_SCHEMA_DEFINITION   ├
   10. ─ Verify it's not an Avro schema (starts with {"type":"record"})   
   11. └─ Error if invalid (400)5. Data Validation (if validation enabled) 
   12. ├─ Convert event.data to JsonNode 
   13. ├─ Validate against JSON Schema   
   13. └─ Error if validation fails (400)6. Topic Name Construction   └─ Format: {kafka.ingressTopicPrefix}.{domain}.{eventName}      Example: "wh.ingress.payments.transactionCreated"7. Create EventEnvelope   ├─ eventId   ├─ SchemaReference   ├─ Metadata (type, source, subject, specVersion, time)   ├─ Timestamp   ├─ Headers (Idempotency-Key, Event-Type, Source)   └─ Format: JSON_SCHEMA8. Publish to Kafka   ├─ Publish as JSON string   └─ Returns published event ID9. Record in Idempotency Ledger   ├─ EVENT_ID: publishedEventId   ├─ EVENT_STATUS: EVENT_READY_FOR_DELIVERY   └─ EVENT_SCHEMAID: SCHEMA_{DOMAIN}_{EVENT}_{VERSION}10. Error Handling    ├─ Validation errors → 400 BAD_REQUEST    ├─ Schema not found → 404 NOT_FOUND    ├─ DynamoDB errors → 500/503    ├─ Kafka errors → 503 SERVICE_UNAVAILABLE    └─ Record failures in ledger (EVENT_PROCESSING_FAILED / EVENT_DELIVERY_FAILED)11. Return Response    └─ 202 Accepted with AckResponse containing eventId
               Special Features:
               Validation can be disabled via webhooks.validation.enabled: false
               Uses CloudEvents id as event ID
               Maps CloudEvents fields to DynamoDB schema lookup
               Records status in EVENT_IDEMPOTENCY_LEDGER
2. POST /webhook/event/publisher - Generic Format
   Method: publishEvent()
   Flow:
1. Extract Event ID & Idempotency Key   
   ├─ Use X-Event-Id header OR generate UUID   └─ Use Idempotency-Key header OR eventId as fallback2. Parse Request Body   ├─ Extract domain, eventName, version, data from InlineObject   └─ Convert data to JSON string3. Schema Lookup   ├─ Create SchemaReference(domain, eventName, version)   ├─ Fetch schema from DynamoDB (with caching)   └─ Error if schema not found (404)4. Format Detection   ├─ Determine Content-Type (JSON/XML/Avro)   └─ Convert payload to JsonNode5. Schema Format Branching   ├─ If JSON_SCHEMA format:   │   ├─ Validate against JSON Schema (if enabled)   │   ├─ Publish as JSON string   │   └─ Topic: {prefix}.{domain}.{eventName}   │   └─ If AVRO_SCHEMA format:       ├─ Coerce JSON types to Avro types       ├─ Validate against Avro Schema (if enabled)       ├─ Serialize to Avro binary       └─ Publish as Avro bytes6. Topic Name Construction   └─ Format: {kafka.ingressTopicPrefix}.{domain}.{eventName}7. Create EventEnvelope   ├─ eventId   ├─ SchemaReference   ├─ Metadata (originalFormat)   ├─ Timestamp   ├─ Headers (Idempotency-Key, Original-Format)   └─ Format: JSON_SCHEMA or AVRO_SCHEMA8. Publish to Kafka   ├─ JSON: publishJson(envelope, topicName, validatedJson)   └─ Avro: publishAvro(envelope, topicName, avroBytes)9. Return Response   └─ 202 Accepted with AckResponse containing eventId
      Special Features:
      Supports JSON, XML, and Avro input formats
      Auto-detects schema format (JSON_SCHEMA vs AVRO_SCHEMA)
      Handles type coercion for Avro (important for XML input)
      Validation can be disabled
3. POST /webhook/schema/schema_id/{schemaId} - By Schema ID
   Method: publishEventBySchemaId()
   Flow:
1. Extract Event ID & Idempotency Key   ├─ Use X-Event-Id header OR generate UUID   └─ Use Idempotency-Key header OR eventId as fallback2. Parse Request Body   ├─ Convert Object to Map<String, Object>   └─ Convert to JSON string3. Schema Lookup by Schema ID   ├─ Fetch schema by EVENT_SCHEMA_ID from DynamoDB   └─ Error if schema not found (404)4. Schema Status Validation   ├─ Check EVENT_SCHEMA_STATUS == "ACTIVE"   ├─ Check TOPIC_STATUS == "ACTIVE"   └─ Check TOPIC_NAME exists   └─ Error if any check fails (400)5. Schema Definition Validation   ├─ Check at least one schema exists (JSON or Avro)   └─ Error if missing (400)6. Format Detection & Conversion   ├─ Determine Content-Type (JSON/XML/Avro)   └─ Convert payload to JsonNode7. Schema Format Branching   ├─ If JSON_SCHEMA format:   │   ├─ Validate against JSON Schema (if enabled)   │   ├─ Publish as JSON string   │   └─ Use topicName from schema   │   └─ If AVRO_SCHEMA format:       ├─ Coerce JSON types to Avro types       ├─ Validate against Avro Schema (if enabled)       ├─ Serialize to Avro binary       └─ Use topicName from schema8. Create EventEnvelope   ├─ eventId   ├─ SchemaReference (from schema)   ├─ Metadata (schemaId)   ├─ Timestamp   ├─ Headers (Idempotency-Key, Original-Format, Schema-Id)   └─ Format: JSON_SCHEMA or AVRO_SCHEMA9. Publish to Kafka   ├─ JSON: publishJson(envelope, topicName, validatedJson)   └─ Avro: publishAvro(envelope, topicName, avroBytes)10. Return Response    └─ 202 Accepted with AckResponse containing eventId
   Special Features:
   Uses EVENT_SCHEMA_ID for direct schema lookup
   Validates schema and topic status (must be ACTIVE)
   Uses topicName from DynamoDB schema record
   Supports JSON, XML, and Avro input formats
   Common Logic Across All Endpoints
   Validation Flow (if enabled):
   failure
1. Check webhooks.validation.enabled flag   ├─ If false → Skip validation, proceed to publish   └─ If true → Continue with validation2. JSON Schema Validation:   ├─ Parse JSON Schema from EVENT_SCHEMA_DEFINITION   ├─ Validate payload against schema   └─ Return detailed error messages on failure3. Avro Schema Validation:   ├─ Parse Avro Schema from EVENT_SCHEMA_DEFINITION_AVRO   ├─ Coerce JSON types to match Avro types   ├─ Validate payload against schema   └─ Return detailed error messages on failure
   Error Handling:
- 400 BAD_REQUEST: Validation failures, missing fields, inactive schema/topic- 404 NOT_FOUND: Schema not found- 500 INTERNAL_SERVER_ERROR: DynamoDB table not found- 503 SERVICE_UNAVAILABLE: Kafka unavailable, DynamoDB unavailable
  Kafka Publishing:
- JSON Format:  ├─ Serialize as JSON string  └─ publishJson(envelope, topicName, jsonNode)- Avro Format:  ├─ Serialize to Avro binary  └─ publishAvro(envelope, topicName, avroBytes)
  Idempotency Ledger (for /webhook/event/publisherCE only):
- Records event status in EVENT_IDEMPOTENCY_LEDGER table- Statuses: EVENT_READY_FOR_DELIVERY, EVENT_PROCESSING_FAILED, EVENT_DELIVERY_FAILED- Fields: EVENT_ID (PK), EVENT_STATUS (SK), EVENT_SCHEMAID, timestamps
  All three endpoints follow this pattern with endpoint-specific variations.
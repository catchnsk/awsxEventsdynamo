package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.beewaxus.webhooksvcs.api.DefaultApi;
import com.beewaxus.webhooksvcs.api.model.AckResponse;
import com.beewaxus.webhooksvcs.api.model.SchemaMetadata;
import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.beewaxus.webhooksvcs.pubsrc.converter.AvroSerializer;
import com.beewaxus.webhooksvcs.pubsrc.converter.FormatConverter;
import com.beewaxus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.beewaxus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.beewaxus.webhooksvcs.pubsrc.publisher.KafkaPublishException;
import com.beewaxus.webhooksvcs.pubsrc.schema.DynamoDbException;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDetailResponse;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormat;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import com.beewaxus.webhooksvcs.pubsrc.validation.AvroSchemaValidator;
import com.beewaxus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormatType;
import jakarta.validation.constraints.NotBlank;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@RestController
@Validated
public class EventController implements DefaultApi {

    private final SchemaService schemaService;
    private final AvroSchemaValidator avroSchemaValidator;
    private final JsonSchemaValidator jsonSchemaValidator;
    private final FormatConverter formatConverter;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final AvroSerializer avroSerializer;
    private final WebhooksProperties properties;

    public EventController(SchemaService schemaService,
                           AvroSchemaValidator avroSchemaValidator,
                           JsonSchemaValidator jsonSchemaValidator,
                           FormatConverter formatConverter,
                           EventPublisher eventPublisher,
                           ObjectMapper objectMapper,
                           AvroSerializer avroSerializer,
                           WebhooksProperties properties) {
        this.schemaService = schemaService;
        this.avroSchemaValidator = avroSchemaValidator;
        this.jsonSchemaValidator = jsonSchemaValidator;
        this.formatConverter = formatConverter;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.avroSerializer = avroSerializer;
        this.properties = properties;
    }

    @Override
    public Mono<ResponseEntity<AckResponse>> publishEvent(
            String domain,
            String eventName,
            String version,
            Mono<Map<String, Object>> requestBody,
            String contentType,
            UUID xEventId,
            String idempotencyKey,
            ServerWebExchange exchange) {

        SchemaReference reference = new SchemaReference(domain, eventName, version);
        String eventId = xEventId != null ? xEventId.toString() : UUID.randomUUID().toString();
        String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
        contentType = contentType != null ? contentType : MediaType.APPLICATION_JSON_VALUE;
        SchemaFormat format = SchemaFormat.fromContentType(contentType);

        return requestBody
                .switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "Body required")))
                .flatMap(bodyMap -> Mono.fromCallable(() -> objectMapper.writeValueAsString(bodyMap))
                        .onErrorMap(e -> new ResponseStatusException(BAD_REQUEST, "Invalid JSON payload: " + e.getMessage())))
                .flatMap(payload -> schemaService.fetchSchema(reference)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found")))
                        .onErrorMap(DynamoDbException.class, e -> {
                            // Check if it's a table not found error
                            if (e.getMessage().contains("does not exist")) {
                                return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                            }
                            // Otherwise, it's a service unavailable error
                            return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                        })
                        .flatMap(schemaDefinition -> {
                            // Construct topic name: {prefix}.{domain}.{eventName}
                            String topicName = "%s.%s.%s".formatted(
                                    properties.kafka().ingressTopicPrefix(),
                                    reference.domain(),
                                    reference.eventName()
                            );

                            // Step 1: Convert payload to JSON
                            return convertToJson(payload, format)
                                    .flatMap(jsonNode -> {
                                        // Branch based on schema format type
                                        if (schemaDefinition.formatType() == SchemaFormatType.JSON_SCHEMA) {
                                            // JSON Schema validation flow
                                            return handleJsonSchemaValidation(jsonNode, schemaDefinition, reference,
                                                    eventId, idempotencyKeyValue, format, topicName);
                                        } else {
                                            // Avro Schema validation flow (existing behavior)
                                            return handleAvroSchemaValidation(jsonNode, schemaDefinition, reference,
                                                    eventId, idempotencyKeyValue, format, topicName);
                                        }
                                    });
                        })
                        .onErrorMap(KafkaPublishException.class, e ->
                            new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + e.getMessage(), e)))
                .map(id -> ResponseEntity.accepted().body(new AckResponse(UUID.fromString(id))));
    }

    @Override
    public Mono<ResponseEntity<AckResponse>> publishEventBySchemaId(
            String schemaId,
            Mono<Map<String, Object>> requestBody,
            String contentType,
            UUID xEventId,
            String idempotencyKey,
            ServerWebExchange exchange) {

        String eventId = xEventId != null ? xEventId.toString() : UUID.randomUUID().toString();
        String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
        contentType = contentType != null ? contentType : MediaType.APPLICATION_JSON_VALUE;
        SchemaFormat format = SchemaFormat.fromContentType(contentType);

        return requestBody
                .switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "Body required")))
                .flatMap(bodyMap -> Mono.fromCallable(() -> objectMapper.writeValueAsString(bodyMap))
                        .onErrorMap(e -> new ResponseStatusException(BAD_REQUEST, "Invalid JSON payload: " + e.getMessage())))
                .flatMap(payload -> schemaService.fetchSchemaBySchemaId(schemaId)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found for schemaId: " + schemaId)))
                        .onErrorMap(DynamoDbException.class, e -> {
                            // Check if it's a table not found error
                            if (e.getMessage().contains("does not exist")) {
                                return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                            }
                            // Otherwise, it's a service unavailable error
                            return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                        })
                        .flatMap(schemaDetail -> {
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
                            
                            // Convert SchemaDetailResponse to SchemaDefinition for validation
                            SchemaDefinition schemaDefinition = schemaDetailToDefinition(schemaDetail);

                            // Validate that at least one schema definition exists
                            boolean hasJsonSchema = schemaDefinition.jsonSchema() != null && !schemaDefinition.jsonSchema().isEmpty();
                            boolean hasAvroSchema = schemaDefinition.avroSchema() != null && !schemaDefinition.avroSchema().isEmpty();

                            if (!hasJsonSchema && !hasAvroSchema) {
                                return Mono.error(new ResponseStatusException(BAD_REQUEST, "Schema definition not configured for this event"));
                            }

                            // Create SchemaReference for EventEnvelope
                            SchemaReference reference = new SchemaReference(
                                    schemaDetail.producerDomain(),
                                    schemaDetail.eventName(),
                                    schemaDetail.version()
                            );

                            // Step 1: Convert payload to JSON
                            return convertToJson(payload, format)
                                    .flatMap(jsonNode -> {
                                        // Branch based on schema format type
                                        if (schemaDefinition.formatType() == SchemaFormatType.JSON_SCHEMA) {
                                            // JSON Schema validation flow
                                            return handleJsonSchemaValidationBySchemaId(jsonNode, schemaDefinition, reference,
                                                    eventId, idempotencyKeyValue, format, schemaDetail.topicName(), schemaId);
                                        } else {
                                            // Avro Schema validation flow
                                            return handleAvroSchemaValidationBySchemaId(jsonNode, schemaDefinition, reference,
                                                    eventId, idempotencyKeyValue, format, schemaDetail.topicName(), schemaId, schemaDefinition.avroSchema());
                                        }
                                    })
                                    .onErrorMap(KafkaPublishException.class, e -> 
                                        new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + e.getMessage(), e))
                                    .onErrorMap(throwable -> {
                                        // Catch any other Kafka-related exceptions
                                        if (throwable.getCause() instanceof KafkaPublishException) {
                                            KafkaPublishException kafkaEx = (KafkaPublishException) throwable.getCause();
                                            return new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + kafkaEx.getMessage(), kafkaEx);
                                        }
                                        // Check for Kafka connection errors
                                        String errorMessage = throwable.getMessage();
                                        if (errorMessage != null && (errorMessage.contains("Kafka") || 
                                                                     errorMessage.contains("broker") || 
                                                                     errorMessage.contains("connection") ||
                                                                     errorMessage.contains("timeout"))) {
                                            return new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + errorMessage, throwable);
                                        }
                                        // Re-throw if not Kafka-related
                                        return throwable;
                                    });
                        }))
                .map(id -> ResponseEntity.accepted().body(new AckResponse(UUID.fromString(id))))
                .onErrorMap(throwable -> {
                    // Catch any unhandled exceptions and ensure they're properly mapped
                    if (throwable instanceof ResponseStatusException) {
                        return throwable; // Already a ResponseStatusException, pass through
                    }
                    if (throwable instanceof KafkaPublishException) {
                        return new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + throwable.getMessage(), throwable);
                    }
                    if (throwable.getCause() instanceof KafkaPublishException) {
                        KafkaPublishException kafkaEx = (KafkaPublishException) throwable.getCause();
                        return new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + kafkaEx.getMessage(), kafkaEx);
                    }
                    // Check for Kafka-related errors in message
                    String errorMessage = throwable.getMessage();
                    if (errorMessage != null && (errorMessage.contains("Kafka") || 
                                                 errorMessage.contains("broker") || 
                                                 errorMessage.contains("connection") ||
                                                 errorMessage.contains("timeout"))) {
                        return new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + errorMessage, throwable);
                    }
                    // For other exceptions, wrap in ResponseStatusException with message
                    return new ResponseStatusException(INTERNAL_SERVER_ERROR, 
                            errorMessage != null ? errorMessage : "An unexpected error occurred: " + throwable.getClass().getSimpleName(), 
                            throwable);
                });
    }

    /**
     * Handle JSON Schema validation flow
     */
    private Mono<String> handleJsonSchemaValidation(JsonNode jsonNode, SchemaDefinition schemaDefinition,
                                                     SchemaReference reference, String eventId,
                                                     String idempotencyKey, SchemaFormat format, String topicName) {
        return jsonSchemaValidator.validate(jsonNode, schemaDefinition)
                .flatMap(validatedJson -> {
                    EventEnvelope envelope = new EventEnvelope(
                            eventId,
                            reference,
                            objectMapper.valueToTree(Map.of("originalFormat", format.name())),
                            Instant.now(),
                            Map.of("Idempotency-Key", idempotencyKey, "Original-Format", format.name()),
                            SchemaFormatType.JSON_SCHEMA
                    );
                    return eventPublisher.publishJson(envelope, topicName, validatedJson);
                });
    }

    /**
     * Handle JSON Schema validation flow for schema ID endpoint
     */
    private Mono<String> handleJsonSchemaValidationBySchemaId(JsonNode jsonNode, SchemaDefinition schemaDefinition,
                                                               SchemaReference reference, String eventId,
                                                               String idempotencyKey, SchemaFormat format,
                                                               String topicName, String schemaId) {
        return jsonSchemaValidator.validate(jsonNode, schemaDefinition)
                .flatMap(validatedJson -> {
                    EventEnvelope envelope = new EventEnvelope(
                            eventId,
                            reference,
                            objectMapper.valueToTree(Map.of("schemaId", schemaId)),
                            Instant.now(),
                            Map.of(
                                    "Idempotency-Key", idempotencyKey,
                                    "Original-Format", format.name(),
                                    "Schema-Id", schemaId
                            ),
                            SchemaFormatType.JSON_SCHEMA
                    );
                    return eventPublisher.publishJson(envelope, topicName, validatedJson);
                });
    }

    /**
     * Handle Avro Schema validation flow for schema ID endpoint
     */
    private Mono<String> handleAvroSchemaValidationBySchemaId(JsonNode jsonNode, SchemaDefinition schemaDefinition,
                                                               SchemaReference reference, String eventId,
                                                               String idempotencyKey, SchemaFormat format,
                                                               String topicName, String schemaId, String avroSchemaString) {
        // Parse Avro schema for type coercion
        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

        // Coerce JSON types to match Avro schema
        JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);

        // Convert to Avro and validate against Avro schema
        return avroSchemaValidator.validate(coercedJson, schemaDefinition)
                .flatMap(avroRecord -> {
                    // Serialize validated Avro record to binary
                    return avroSerializer.serializeToAvro(avroRecord, avroSchema);
                })
                .flatMap(avroBytes -> {
                    EventEnvelope envelope = new EventEnvelope(
                            eventId,
                            reference,
                            objectMapper.valueToTree(Map.of("schemaId", schemaId)),
                            Instant.now(),
                            Map.of(
                                    "Idempotency-Key", idempotencyKey,
                                    "Original-Format", format.name(),
                                    "Schema-Id", schemaId
                            ),
                            SchemaFormatType.AVRO_SCHEMA
                    );
                    return eventPublisher.publishAvro(envelope, topicName, avroBytes);
                });
    }

    /**
     * Handle Avro Schema validation flow (existing behavior)
     */
    private Mono<String> handleAvroSchemaValidation(JsonNode jsonNode, SchemaDefinition schemaDefinition,
                                                     SchemaReference reference, String eventId,
                                                     String idempotencyKey, SchemaFormat format, String topicName) {
        String avroSchemaString = schemaDefinition.avroSchema();

        // Validate Avro schema exists
        if (avroSchemaString == null || avroSchemaString.isEmpty()) {
            return Mono.error(new ResponseStatusException(BAD_REQUEST, "Avro schema definition not configured for this event"));
        }

        // Parse Avro schema for type coercion
        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

        // Coerce JSON types to match Avro schema (important for XML where all values are strings)
        JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);

        // Convert to Avro and validate against Avro schema
        return avroSchemaValidator.validate(coercedJson, schemaDefinition)
                .flatMap(avroRecord -> {
                    // Serialize validated Avro record to binary
                    return avroSerializer.serializeToAvro(avroRecord, avroSchema);
                })
                .flatMap(avroBytes -> {
                    EventEnvelope envelope = new EventEnvelope(
                            eventId,
                            reference,
                            objectMapper.valueToTree(Map.of("originalFormat", format.name())),
                            Instant.now(),
                            Map.of("Idempotency-Key", idempotencyKey, "Original-Format", format.name()),
                            SchemaFormatType.AVRO_SCHEMA
                    );
                    return eventPublisher.publishAvro(envelope, topicName, avroBytes);
                });
    }

    /**
     * Convert payload to JSON format based on Content-Type.
     * For JSON/XML/Avro, all are converted to JsonNode for further processing.
     * Validation against Avro schema happens later in the flow.
     */
    private Mono<JsonNode> convertToJson(String payload, SchemaFormat format) {
        return switch (format) {
            case JSON -> Mono.fromCallable(() -> objectMapper.readTree(payload))
                    .subscribeOn(Schedulers.boundedElastic());
            case XML -> formatConverter.xmlToJson(payload);
            case AVRO -> Mono.fromCallable(() -> objectMapper.readTree(payload))
                    .subscribeOn(Schedulers.boundedElastic());
        };
    }

    private SchemaDefinition schemaDetailToDefinition(SchemaDetailResponse detail) {
        SchemaReference reference = new SchemaReference(
                detail.producerDomain(),
                detail.eventName(),
                detail.version()
        );

        String jsonSchema = detail.eventSchemaDefinition();
        String avroSchema = detail.eventSchemaDefinitionAvro();

        // Determine schema format type
        SchemaFormatType formatType;
        if (jsonSchema != null && !jsonSchema.isEmpty()) {
            formatType = SchemaFormatType.JSON_SCHEMA;
        } else if (avroSchema != null && !avroSchema.isEmpty()) {
            formatType = SchemaFormatType.AVRO_SCHEMA;
        } else {
            formatType = SchemaFormatType.AVRO_SCHEMA; // Default fallback
        }

        return new SchemaDefinition(
                reference,
                jsonSchema,
                avroSchema,
                formatType,
                "ACTIVE".equals(detail.eventSchemaStatus()),
                detail.updateTs()
        );
    }

    private Mono<GenericRecord> convertJsonToAvroRecord(JsonNode jsonNode, Schema avroSchema) {
        return Mono.fromCallable(() -> {
                    // Convert JsonNode to Avro-compatible JSON with proper type coercion
                    JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);
                    String jsonString = coercedJson.toString();
                    
                    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
                    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, jsonString);
                    return reader.read(null, decoder);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private JsonNode coerceJsonToAvroTypes(JsonNode jsonNode, Schema avroSchema) {
        if (avroSchema.getType() == Schema.Type.RECORD) {
            com.fasterxml.jackson.databind.node.ObjectNode result = objectMapper.createObjectNode();
            
            for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                String fieldName = field.name();
                JsonNode fieldValue = jsonNode.get(fieldName);
                
                if (fieldValue != null && !fieldValue.isNull()) {
                    JsonNode coercedValue = coerceValueToAvroType(fieldValue, field.schema());
                    result.set(fieldName, coercedValue);
                }
                // Note: Default values are handled by Avro during deserialization
            }
            
            return result;
        }
        
        return coerceValueToAvroType(jsonNode, avroSchema);
    }

    private JsonNode coerceValueToAvroType(JsonNode value, Schema schema) {
        return switch (schema.getType()) {
            case STRING -> objectMapper.valueToTree(value.asText());
            case INT -> objectMapper.valueToTree(value.asInt());
            case LONG -> objectMapper.valueToTree(value.asLong());
            case FLOAT -> objectMapper.valueToTree((float) value.asDouble());
            case DOUBLE -> {
                // Handle string-to-double conversion (common from XML)
                if (value.isTextual()) {
                    try {
                        yield objectMapper.valueToTree(Double.parseDouble(value.asText()));
                    } catch (NumberFormatException e) {
                        yield objectMapper.valueToTree(value.asDouble());
                    }
                }
                yield objectMapper.valueToTree(value.asDouble());
            }
            case BOOLEAN -> objectMapper.valueToTree(value.asBoolean());
            case RECORD -> {
                if (value.isObject()) {
                    com.fasterxml.jackson.databind.node.ObjectNode recordNode = objectMapper.createObjectNode();
                    for (org.apache.avro.Schema.Field field : schema.getFields()) {
                        JsonNode fieldValue = value.get(field.name());
                        if (fieldValue != null && !fieldValue.isNull()) {
                            recordNode.set(field.name(), coerceValueToAvroType(fieldValue, field.schema()));
                        }
                    }
                    yield recordNode;
                }
                yield value;
            }
            case ARRAY -> {
                if (value.isArray()) {
                    com.fasterxml.jackson.databind.node.ArrayNode arrayNode = objectMapper.createArrayNode();
                    Schema elementSchema = schema.getElementType();
                    value.forEach(element -> arrayNode.add(coerceValueToAvroType(element, elementSchema)));
                    yield arrayNode;
                }
                yield value;
            }
            case MAP -> {
                if (value.isObject()) {
                    com.fasterxml.jackson.databind.node.ObjectNode mapNode = objectMapper.createObjectNode();
                    Schema valueSchema = schema.getValueType();
                    value.fields().forEachRemaining(entry -> 
                        mapNode.set(entry.getKey(), coerceValueToAvroType(entry.getValue(), valueSchema)));
                    yield mapNode;
                }
                yield value;
            }
            case UNION -> {
                // For union types, try each schema type until one works
                JsonNode result = value; // Default fallback
                for (Schema unionSchema : schema.getTypes()) {
                    if (unionSchema.getType() == Schema.Type.NULL && value.isNull()) {
                        result = objectMapper.nullNode();
                        break;
                    }
                    try {
                        JsonNode coerced = coerceValueToAvroType(value, unionSchema);
                        if (coerced != null && !coerced.isNull()) {
                            result = coerced;
                            break;
                        }
                    } catch (Exception e) {
                        // Try next union type
                    }
                }
                yield result;
            }
            default -> value;
        };
    }

    // Additional methods from DefaultApi interface

    @Override
    public Mono<ResponseEntity<SchemaMetadata>> fetchSchema(String domain, String event, String version, ServerWebExchange exchange) {
        // TODO: Implement fetchSchema method
        return Mono.error(new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED, "fetchSchema not yet implemented"));
    }

    @Override
    public Mono<ResponseEntity<Flux<SchemaMetadata>>> getAllSchemas(ServerWebExchange exchange) {
        // TODO: Implement getAllSchemas method
        return Mono.error(new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED, "getAllSchemas not yet implemented"));
    }

    @Override
    public Mono<ResponseEntity<com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse>> getSchemaBySchemaId(String schemaId, ServerWebExchange exchange) {
        // TODO: Implement getSchemaBySchemaId method
        return Mono.error(new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED, "getSchemaBySchemaId not yet implemented"));
    }
}

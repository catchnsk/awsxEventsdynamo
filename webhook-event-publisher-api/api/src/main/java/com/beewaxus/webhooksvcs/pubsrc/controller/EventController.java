package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import jakarta.validation.constraints.NotBlank;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
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
public class EventController {

    private final SchemaService schemaService;
    private final AvroSchemaValidator avroSchemaValidator;
    private final FormatConverter formatConverter;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final AvroSerializer avroSerializer;
    private final WebhooksProperties properties;

    public EventController(SchemaService schemaService,
                           AvroSchemaValidator avroSchemaValidator,
                           FormatConverter formatConverter,
                           EventPublisher eventPublisher,
                           ObjectMapper objectMapper,
                           AvroSerializer avroSerializer,
                           WebhooksProperties properties) {
        this.schemaService = schemaService;
        this.avroSchemaValidator = avroSchemaValidator;
        this.formatConverter = formatConverter;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.avroSerializer = avroSerializer;
        this.properties = properties;
    }

    @PostMapping(value = "/events/{eventName}",
                 consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, "application/avro"})
    public Mono<ResponseEntity<EventAcceptedResponse>> publishEvent(
            @PathVariable @NotBlank String eventName,
            @RequestHeader("X-Producer-Domain") String domain,
            @RequestHeader("X-Event-Version") String version,
            @RequestHeader(value = "Content-Type", defaultValue = MediaType.APPLICATION_JSON_VALUE) String contentType,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody String rawPayload,
            @RequestHeader Map<String, String> headers) {

        SchemaReference reference = new SchemaReference(domain, eventName, version);
        String eventId = headers.getOrDefault("X-Event-Id", UUID.randomUUID().toString());
        String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
        SchemaFormat format = SchemaFormat.fromContentType(contentType);

        return Mono.just(rawPayload)
                .switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "Body required")))
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
                            // Get Avro schema (prefer EVENT_SCHEMA_DEFINITION, fallback to EVENT_SCHEMA_DEFINITION_AVRO)
                            // For this endpoint, we need to fetch the full schema detail to get EVENT_SCHEMA_DEFINITION
                            String avroSchemaString = schemaDefinition.avroSchema();
                            
                            // Validate Avro schema exists
                            if (avroSchemaString == null || avroSchemaString.isEmpty()) {
                                return Mono.error(new ResponseStatusException(BAD_REQUEST, "Schema definition not configured for this event"));
                            }
                            
                            // Step 1: Convert payload to JSON (if XML/JSON/Avro)
                            return convertToJson(payload, format)
                                    .flatMap(jsonNode -> {
                                        // Step 2: Parse Avro schema for type coercion
                                        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
                                        
                                        // Step 3: Coerce JSON types to match Avro schema (important for XML where all values are strings)
                                        JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);
                                        
                                        // Step 4: Convert to Avro and validate against Avro schema
                                        // This will throw SchemaValidationException if validation fails
                                        return avroSchemaValidator.validate(coercedJson, schemaDefinition);
                                    })
                                    .flatMap(avroRecord -> {
                                        // Step 5: Serialize validated Avro record to binary
                                        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
                                        return avroSerializer.serializeToAvro(avroRecord, avroSchema);
                                    })
                                    .flatMap(avroBytes -> {
                                        // Step 4: Publish to Kafka (construct topic name from schema reference)
                                        EventEnvelope envelope = new EventEnvelope(
                                                eventId,
                                                reference,
                                                objectMapper.valueToTree(Map.of("originalFormat", format.name())),
                                                Instant.now(),
                                                Map.of("Idempotency-Key", idempotencyKeyValue, "Original-Format", format.name())
                                        );
                                        // Construct topic name: {prefix}.{domain}.{eventName}
                                        String topicName = "%s.%s.%s".formatted(
                                                properties.kafka().ingressTopicPrefix(),
                                                reference.domain(),
                                                reference.eventName()
                                        );
                                        return eventPublisher.publishAvro(envelope, topicName, avroBytes);
                                    });
                        })
                        .onErrorMap(KafkaPublishException.class, e -> 
                            new ResponseStatusException(SERVICE_UNAVAILABLE, "Kafka is unavailable: " + e.getMessage(), e)))
                .map(id -> ResponseEntity.accepted().body(new EventAcceptedResponse(id)));
    }

    @PostMapping(value = "/events/schema_id/{schemaId}",
                 consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, "application/avro"})
    public Mono<ResponseEntity<EventAcceptedResponse>> publishEventBySchemaId(
            @PathVariable @NotBlank String schemaId,
            @RequestHeader(value = "Content-Type", defaultValue = MediaType.APPLICATION_JSON_VALUE) String contentType,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody String rawPayload,
            @RequestHeader Map<String, String> headers) {

        String eventId = headers.getOrDefault("X-Event-Id", UUID.randomUUID().toString());
        String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
        SchemaFormat format = SchemaFormat.fromContentType(contentType);

        return Mono.just(rawPayload)
                .switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "Body required")))
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
                            
                            // Validate schema definition exists (prefer EVENT_SCHEMA_DEFINITION, fallback to EVENT_SCHEMA_DEFINITION_AVRO)
                            String avroSchemaString = schemaDetail.eventSchemaDefinition() != null && !schemaDetail.eventSchemaDefinition().isEmpty()
                                    ? schemaDetail.eventSchemaDefinition()
                                    : schemaDetail.eventSchemaDefinitionAvro();
                            
                            if (avroSchemaString == null || avroSchemaString.isEmpty()) {
                                return Mono.error(new ResponseStatusException(BAD_REQUEST, "Schema definition not configured for this event"));
                            }
                            
                            // Convert SchemaDetailResponse to SchemaDefinition for validation
                            SchemaDefinition schemaDefinition = schemaDetailToDefinition(schemaDetail);
                            
                            // Create SchemaReference for EventEnvelope
                            SchemaReference reference = new SchemaReference(
                                    schemaDetail.producerDomain(),
                                    schemaDetail.eventName(),
                                    schemaDetail.version()
                            );
                            
                            // Step 1: Convert payload to JSON (if XML/JSON/Avro)
                            return convertToJson(payload, format)
                                    .flatMap(jsonNode -> {
                                        // Step 2: Parse Avro schema for type coercion
                                        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
                                        
                                        // Step 3: Coerce JSON types to match Avro schema (important for XML where all values are strings)
                                        JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);
                                        
                                        // Step 4: Convert to Avro and validate against Avro schema
                                        // This will throw SchemaValidationException if validation fails
                                        return avroSchemaValidator.validate(coercedJson, schemaDefinition);
                                    })
                                    .flatMap(avroRecord -> {
                                        // Step 5: Serialize validated Avro record to binary
                                        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
                                        return avroSerializer.serializeToAvro(avroRecord, avroSchema);
                                    })
                                    .flatMap(avroBytes -> {
                                        // Create EventEnvelope
                                        EventEnvelope envelope = new EventEnvelope(
                                                eventId,
                                                reference,
                                                objectMapper.valueToTree(Map.of("schemaId", schemaId)),
                                                Instant.now(),
                                                Map.of(
                                                        "Idempotency-Key", idempotencyKeyValue,
                                                        "Original-Format", format.name(),
                                                        "Schema-Id", schemaId
                                                )
                                        );
                                        
                                        // Publish to Kafka using topic from schema
                                        return eventPublisher.publishAvro(envelope, schemaDetail.topicName(), avroBytes);
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
                .map(id -> ResponseEntity.accepted().body(new EventAcceptedResponse(id)))
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
        
        // Prefer EVENT_SCHEMA_DEFINITION, fallback to EVENT_SCHEMA_DEFINITION_AVRO
        String avroSchema = detail.eventSchemaDefinition() != null && !detail.eventSchemaDefinition().isEmpty()
                ? detail.eventSchemaDefinition()
                : detail.eventSchemaDefinitionAvro();
        
        return new SchemaDefinition(
                reference,
                avroSchema,
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

    public record EventAcceptedResponse(String eventId) {}
}

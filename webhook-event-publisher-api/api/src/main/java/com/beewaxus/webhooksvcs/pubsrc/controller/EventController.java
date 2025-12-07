package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.beewaxus.webhooksvcs.api.DefaultApi;
import com.beewaxus.webhooksvcs.api.model.AckResponse;
import com.beewaxus.webhooksvcs.api.model.CloudEvent;
import com.beewaxus.webhooksvcs.api.model.SchemaMetadata;
import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.beewaxus.webhooksvcs.pubsrc.ledger.IdempotencyLedgerService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(EventController.class);

    private final SchemaService schemaService;
    private final AvroSchemaValidator avroSchemaValidator;
    private final JsonSchemaValidator jsonSchemaValidator;
    private final FormatConverter formatConverter;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final AvroSerializer avroSerializer;
    private final WebhooksProperties properties;
    private final IdempotencyLedgerService idempotencyLedgerService;

    public EventController(SchemaService schemaService,
                           AvroSchemaValidator avroSchemaValidator,
                           JsonSchemaValidator jsonSchemaValidator,
                           FormatConverter formatConverter,
                           EventPublisher eventPublisher,
                           ObjectMapper objectMapper,
                           AvroSerializer avroSerializer,
                           WebhooksProperties properties,
                           IdempotencyLedgerService idempotencyLedgerService) {
        this.schemaService = schemaService;
        this.avroSchemaValidator = avroSchemaValidator;
        this.jsonSchemaValidator = jsonSchemaValidator;
        this.formatConverter = formatConverter;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.avroSerializer = avroSerializer;
        this.properties = properties;
        this.idempotencyLedgerService = idempotencyLedgerService;
    }

    @Override
    public Mono<ResponseEntity<AckResponse>> publishEvent(
            Mono<com.beewaxus.webhooksvcs.api.model.InlineObject> inlineObject,
            String contentType,
            UUID xEventId,
            String idempotencyKey,
            ServerWebExchange exchange) {

        String eventId = xEventId != null ? xEventId.toString() : UUID.randomUUID().toString();
        String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;
        contentType = contentType != null ? contentType : MediaType.APPLICATION_JSON_VALUE;
        SchemaFormat format = SchemaFormat.fromContentType(contentType);

        return inlineObject
                .switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "Body required")))
                .flatMap(body -> {
                    // Extract domain, eventName, version, and data from the body
                    String domain = body.getDomain();
                    String eventName = body.getEventName();
                    String version = body.getVersion();
                    Map<String, Object> data = body.getData();

                    SchemaReference reference = new SchemaReference(domain, eventName, version);

                    // Convert data to JSON string
                    return Mono.fromCallable(() -> objectMapper.writeValueAsString(data))
                            .onErrorMap(e -> new ResponseStatusException(BAD_REQUEST, "Invalid JSON payload: " + e.getMessage()))
                            .flatMap(payload -> schemaService.fetchSchema(reference)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found")))
                        .onErrorMap(DynamoDbException.class, e -> {
                            // Record processing failure for DynamoDB errors (fire and forget)
                            idempotencyLedgerService.recordEventStatus(
                                    eventId,
                                    IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                    null
                            ).subscribe(
                                    null,
                                    error -> log.error("Failed to record processing failure in ledger", error)
                            );
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
                                    })
                                    .flatMap(publishedEventId -> {
                                        // Construct schema ID from reference (format: SCHEMA_{DOMAIN}_{EVENT}_{VERSION})
                                        String schemaId = String.format("SCHEMA_%s_%s_%s",
                                                reference.domain().toUpperCase(),
                                                reference.eventName().toUpperCase(),
                                                reference.version().toUpperCase().replace(".", "_"));
                                        
                                        // Record status in idempotency ledger (non-blocking - don't fail request if ledger write fails)
                                        idempotencyLedgerService.recordEventStatus(
                                                publishedEventId,
                                                IdempotencyLedgerService.EventStatus.EVENT_READY_FOR_DELIVERY,
                                                schemaId
                                        )
                                        .subscribe(
                                                null,
                                                error -> log.warn("Failed to record event status in idempotency ledger for eventId: {}. Error: {}", 
                                                        publishedEventId, error.getMessage())
                                        );
                                        return Mono.just(publishedEventId);
                                    })
                                    .onErrorResume(KafkaPublishException.class, e -> {
                                        // Record delivery failure in ledger (fire and forget)
                                        idempotencyLedgerService.recordEventStatus(
                                                eventId,
                                                IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                                                null
                                        ).subscribe(
                                                null,
                                                error -> log.error("Failed to record delivery failure in ledger", error)
                                        );
                                        return Mono.error(new ResponseStatusException(SERVICE_UNAVAILABLE, 
                                                "Kafka is unavailable: " + e.getMessage(), e));
                                    })
                                    .onErrorResume(throwable -> {
                                        // Record processing failure (fire and forget) for validation errors
                                        if (!(throwable instanceof KafkaPublishException)) {
                                            idempotencyLedgerService.recordEventStatus(
                                                    eventId,
                                                    IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                                    null
                                            ).subscribe(
                                                    null,
                                                    error -> log.error("Failed to record processing failure in ledger", error)
                                            );
                                        }
                                        // Re-throw the error
                                        if (throwable instanceof ResponseStatusException) {
                                            return Mono.error(throwable);
                                        }
                                        return Mono.error(new ResponseStatusException(BAD_REQUEST, 
                                                throwable.getMessage() != null ? throwable.getMessage() : "Processing failed", 
                                                throwable));
                                    });
                        })
                    );
                })
                .map(id -> {
                    AckResponse response = new AckResponse();
                    response.setEventId(UUID.fromString(id));
                    return ResponseEntity.accepted().body(response);
                });
    }

    @Override
    public Mono<ResponseEntity<AckResponse>> publishCloudEvent(
            Mono<CloudEvent> cloudEvent,
            String contentType,
            UUID xEventId,
            String idempotencyKey,
            ServerWebExchange exchange) {

        log.info("publishCloudEvent called - Content-Type: {}, X-Event-Id: {}, Idempotency-Key: {}, Path: {}", 
                contentType, xEventId, idempotencyKey, exchange.getRequest().getPath().value());
        
        return cloudEvent
                .doOnNext(event -> log.info("CloudEvent received - ID: {}, Type: {}, Source: {}, Subject: {}, SpecVersion: {}", 
                        event.getId(), event.getType(), event.getSource(), event.getSubject(), event.getSpecVersion()))
                .doOnError(error -> log.error("Error receiving CloudEvent: {}", error.getMessage(), error))
                .switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "CloudEvent body required")))
                .flatMap(event -> {
                    // Use event.id as the event ID (or override from header if provided)
                    String eventId = xEventId != null ? xEventId.toString() : event.getId().toString();
                    String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;

                    // Validate mandatory fields
                    if (event.getSource() == null || event.getSource().isEmpty()) {
                        return Mono.error(new ResponseStatusException(BAD_REQUEST, "source field is required"));
                    }
                    if (event.getSubject() == null || event.getSubject().isEmpty()) {
                        return Mono.error(new ResponseStatusException(BAD_REQUEST, "subject field is required"));
                    }
                    if (event.getSpecVersion() == null || event.getSpecVersion().isEmpty()) {
                        return Mono.error(new ResponseStatusException(BAD_REQUEST, "specVersion field is required"));
                    }
                    if (event.getData() == null || event.getData().isEmpty()) {
                        return Mono.error(new ResponseStatusException(BAD_REQUEST, "data field is required"));
                    }

                    // Map CloudEvents fields to schema lookup: source → PRODUCER_DOMAIN, subject → EVENT_NAME, specVersion → VERSION
                    SchemaReference reference = new SchemaReference(
                            event.getSource(),  // source → PRODUCER_DOMAIN
                            event.getSubject(), // subject → EVENT_NAME
                            event.getSpecVersion() // specVersion → VERSION
                    );

                    // Fetch schema from DynamoDB
                    return schemaService.fetchSchema(reference)
                            .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, 
                                    "Schema not found for source=" + event.getSource() + 
                                    ", subject=" + event.getSubject() + 
                                    ", specVersion=" + event.getSpecVersion())))
                            .onErrorMap(DynamoDbException.class, e -> {
                                if (e.getMessage().contains("does not exist")) {
                                    return new ResponseStatusException(INTERNAL_SERVER_ERROR, 
                                            "DynamoDB table not found: " + e.getMessage(), e);
                                }
                                return new ResponseStatusException(SERVICE_UNAVAILABLE, 
                                        "DynamoDB service unavailable: " + e.getMessage(), e);
                            })
                            .flatMap(schemaDefinition -> {
                                // Check if validation is enabled FIRST
                                boolean validationEnabled = properties.validation() != null && properties.validation().isEnabled();
                                
                                log.info("Fetched schema definition for source={}, subject={}, specVersion={}. " +
                                        "Has JSON Schema: {}, Has Avro Schema: {}, Format Type: {}, Event Schema ID: {}, Validation Enabled: {}", 
                                        event.getSource(), event.getSubject(), event.getSpecVersion(),
                                        schemaDefinition.jsonSchema() != null && !schemaDefinition.jsonSchema().isEmpty(),
                                        schemaDefinition.avroSchema() != null && !schemaDefinition.avroSchema().isEmpty(),
                                        schemaDefinition.formatType(),
                                        schemaDefinition.eventSchemaId(),
                                        validationEnabled);
                                
                                // Only validate schema existence and format if validation is enabled
                                if (validationEnabled) {
                                    // Validate that JSON Schema exists (required for this endpoint)
                                    if (schemaDefinition.jsonSchema() == null || schemaDefinition.jsonSchema().isEmpty()) {
                                        log.error("JSON Schema (EVENT_SCHEMA_DEFINITION) not configured for source={}, subject={}, specVersion={}", 
                                                event.getSource(), event.getSubject(), event.getSpecVersion());
                                        return Mono.error(new ResponseStatusException(BAD_REQUEST, 
                                                "JSON Schema (EVENT_SCHEMA_DEFINITION) not configured for this event"));
                                    }

                                    // Check if EVENT_SCHEMA_DEFINITION is actually an Avro schema (starts with {"type":"record"})
                                    String jsonSchemaStr = schemaDefinition.jsonSchema();
                                    if (jsonSchemaStr != null && jsonSchemaStr.trim().startsWith("{\"type\":\"record\"")) {
                                        log.error("EVENT_SCHEMA_DEFINITION contains Avro schema instead of JSON Schema for source={}, subject={}, specVersion={}. " +
                                                "Schema preview: {}", 
                                                event.getSource(), event.getSubject(), event.getSpecVersion(),
                                                jsonSchemaStr.length() > 200 ? jsonSchemaStr.substring(0, 200) + "..." : jsonSchemaStr);
                                        return Mono.error(new ResponseStatusException(BAD_REQUEST, 
                                                "EVENT_SCHEMA_DEFINITION contains an Avro schema, but this endpoint requires a JSON Schema. " +
                                                "Please provide a valid JSON Schema in EVENT_SCHEMA_DEFINITION or use EVENT_SCHEMA_DEFINITION_AVRO for Avro schemas."));
                                    }
                                } else {
                                    log.warn("Schema validation is DISABLED (webhooks.validation.enabled=false). " +
                                            "Skipping schema format checks and validation for testing purposes.");
                                }

                                // Convert data to JsonNode
                                JsonNode dataJsonNode = objectMapper.valueToTree(event.getData());
                                
                                log.info("Processing CloudEvent data. Source: {}, Subject: {}, SpecVersion: {}, Data: {}, Validation: {}", 
                                        event.getSource(), event.getSubject(), event.getSpecVersion(), dataJsonNode.toString(), 
                                        validationEnabled ? "ENABLED" : "DISABLED");
                                
                                // Validate data against JSON Schema (or skip if disabled)
                                Mono<JsonNode> validatedJsonMono;
                                if (!validationEnabled) {
                                    log.warn("Skipping JSON Schema validation - validation is disabled for testing.");
                                    validatedJsonMono = Mono.just(dataJsonNode);
                                } else {
                                    String jsonSchemaStr = schemaDefinition.jsonSchema();
                                    if (jsonSchemaStr != null) {
                                        log.info("Using JSON Schema for validation (length: {} chars). Schema preview (first 500 chars): {}", 
                                                jsonSchemaStr.length(), 
                                                jsonSchemaStr.length() > 500 ? jsonSchemaStr.substring(0, 500) + "..." : jsonSchemaStr);
                                    }
                                    validatedJsonMono = jsonSchemaValidator.validate(dataJsonNode, schemaDefinition);
                                }
                                
                                return validatedJsonMono
                                        .flatMap(validatedJson -> {
                                            // Construct topic name: {prefix}.{domain}.{eventName}
                                            String topicName = "%s.%s.%s".formatted(
                                                    properties.kafka().ingressTopicPrefix(),
                                                    reference.domain(),
                                                    reference.eventName()
                                            );

                                            // Create EventEnvelope
                                            EventEnvelope envelope = new EventEnvelope(
                                                    eventId,
                                                    reference,
                                                    objectMapper.valueToTree(Map.of(
                                                            "type", event.getType() != null ? event.getType() : "",
                                                            "source", event.getSource(),
                                                            "subject", event.getSubject(),
                                                            "specVersion", event.getSpecVersion(),
                                                            "time", event.getTime() != null ? event.getTime().toString() : ""
                                                    )),
                                                    event.getTime() != null ? event.getTime().toInstant() : Instant.now(),
                                                    Map.of(
                                                            "Idempotency-Key", idempotencyKeyValue,
                                                            "Event-Type", event.getType() != null ? event.getType() : "",
                                                            "Source", event.getSource()
                                                    ),
                                                    SchemaFormatType.JSON_SCHEMA
                                            );

                                            // Publish to Kafka
                                            return eventPublisher.publishJson(envelope, topicName, validatedJson)
                                                    .flatMap(publishedEventId -> {
                                                        // Construct schema ID from reference (format: SCHEMA_{DOMAIN}_{EVENT}_{VERSION})
                                                        String schemaId = String.format("SCHEMA_%s_%s_%s",
                                                                reference.domain().toUpperCase(),
                                                                reference.eventName().toUpperCase(),
                                                                reference.version().toUpperCase().replace(".", "_"));
                                                        
                                                        // Record status in idempotency ledger (non-blocking - don't fail request if ledger write fails)
                                                        idempotencyLedgerService.recordEventStatus(
                                                                publishedEventId,
                                                                IdempotencyLedgerService.EventStatus.EVENT_READY_FOR_DELIVERY,
                                                                schemaId
                                                        )
                                                        .subscribe(
                                                                null,
                                                                error -> log.warn("Failed to record event status in idempotency ledger for eventId: {}. Error: {}", 
                                                                        publishedEventId, error.getMessage())
                                                        );
                                                        return Mono.just(publishedEventId);
                                                    })
                                                    .onErrorResume(KafkaPublishException.class, e -> {
                                                        // Record failure in ledger (fire and forget)
                                                        idempotencyLedgerService.recordEventStatus(
                                                                eventId,
                                                                IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                                                                null
                                                        ).subscribe(
                                                                null,
                                                                error -> log.error("Failed to record delivery failure in ledger", error)
                                                        );
                                                        return Mono.error(new ResponseStatusException(SERVICE_UNAVAILABLE, 
                                                                "Kafka is unavailable: " + e.getMessage(), e));
                                                    });
                                        })
                                        .onErrorResume(throwable -> {
                                            if (throwable instanceof ResponseStatusException) {
                                                return Mono.error(throwable);
                                            }
                                            
                                            // Extract detailed error message - get root cause
                                            Throwable rootCause = throwable;
                                            while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
                                                rootCause = rootCause.getCause();
                                            }
                                            
                                            String errorMessage = rootCause.getMessage();
                                            if (errorMessage == null || errorMessage.isEmpty()) {
                                                errorMessage = rootCause.getClass().getSimpleName();
                                            }
                                            
                                            // Log full exception details for debugging
                                            log.error("JSON Schema validation failed for CloudEvent. Source: {}, Subject: {}, SpecVersion: {}, " +
                                                    "Error Type: {}, Error Message: {}, Root Cause: {}", 
                                                    event.getSource(), event.getSubject(), event.getSpecVersion(), 
                                                    throwable.getClass().getName(), errorMessage, rootCause.getClass().getName(), throwable);
                                            
                                            // Log the data and schema that failed validation
                                            String schemaStr = schemaDefinition.jsonSchema();
                                            if (schemaStr != null) {
                                                log.error("Failed validation details - Data: {}, Schema length: {}", 
                                                        dataJsonNode.toString(), schemaStr.length());
                                            } else {
                                                log.error("Failed validation details - Data: {}, Schema: null", 
                                                        dataJsonNode.toString());
                                            }
                                            
                                            // Record processing failure (fire and forget)
                                            idempotencyLedgerService.recordEventStatus(
                                                    eventId,
                                                    IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                                    null
                                            ).subscribe(
                                                    null,
                                                    error -> log.error("Failed to record processing failure in ledger", error)
                                            );
                                            
                                            // Return error with full message
                                            return Mono.error(new ResponseStatusException(BAD_REQUEST, 
                                                    errorMessage, throwable));
                                        });
                            });
                })
                .map(publishedEventId -> {
                    AckResponse response = new AckResponse();
                    response.setEventId(UUID.fromString(publishedEventId));
                    return ResponseEntity.accepted().body(response);
                })
                .onErrorMap(throwable -> {
                    if (throwable instanceof ResponseStatusException) {
                        return throwable;
                    }
                    return new ResponseStatusException(INTERNAL_SERVER_ERROR, 
                            throwable.getMessage() != null ? throwable.getMessage() : "An unexpected error occurred", 
                            throwable);
                });
    }

    @Override
    public Mono<ResponseEntity<AckResponse>> publishEventBySchemaId(
            String schemaId,
            Mono<Object> requestBody,
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
                .flatMap(body -> {
                    // Convert Object to Map<String, Object> if needed
                    Map<String, Object> bodyMap;
                    if (body instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) body;
                        bodyMap = map;
                    } else {
                        // Convert to Map using ObjectMapper
                        bodyMap = objectMapper.convertValue(body, Map.class);
                    }
                    return Mono.fromCallable(() -> objectMapper.writeValueAsString(bodyMap))
                            .onErrorMap(e -> new ResponseStatusException(BAD_REQUEST, "Invalid JSON payload: " + e.getMessage()));
                })
                .flatMap(payload -> schemaService.fetchSchemaBySchemaId(schemaId)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found for schemaId: " + schemaId)))
                        .onErrorMap(DynamoDbException.class, e -> {
                            // Record processing failure for DynamoDB errors (fire and forget)
                            idempotencyLedgerService.recordEventStatus(
                                    eventId,
                                    IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                    schemaId
                            ).subscribe(
                                    null,
                                    error -> log.error("Failed to record processing failure in ledger", error)
                            );
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
                                    .flatMap(publishedEventId -> {
                                        // Record status in idempotency ledger (non-blocking - don't fail request if ledger write fails)
                                        idempotencyLedgerService.recordEventStatus(
                                                publishedEventId,
                                                IdempotencyLedgerService.EventStatus.EVENT_READY_FOR_DELIVERY,
                                                schemaId
                                        )
                                        .subscribe(
                                                null,
                                                error -> log.warn("Failed to record event status in idempotency ledger for eventId: {}. Error: {}", 
                                                        publishedEventId, error.getMessage())
                                        );
                                        return Mono.just(publishedEventId);
                                    })
                                    .onErrorResume(KafkaPublishException.class, e -> {
                                        // Record delivery failure in ledger (fire and forget)
                                        idempotencyLedgerService.recordEventStatus(
                                                eventId,
                                                IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                                                schemaId
                                        ).subscribe(
                                                null,
                                                error -> log.error("Failed to record delivery failure in ledger", error)
                                        );
                                        return Mono.error(new ResponseStatusException(SERVICE_UNAVAILABLE, 
                                                "Kafka is unavailable: " + e.getMessage(), e));
                                    })
                                    .onErrorResume(throwable -> {
                                        // Catch any other Kafka-related exceptions
                                        if (throwable.getCause() instanceof KafkaPublishException) {
                                            KafkaPublishException kafkaEx = (KafkaPublishException) throwable.getCause();
                                            // Record delivery failure in ledger (fire and forget)
                                            idempotencyLedgerService.recordEventStatus(
                                                    eventId,
                                                    IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                                                    schemaId
                                            ).subscribe(
                                                    null,
                                                    error -> log.error("Failed to record delivery failure in ledger", error)
                                            );
                                            return Mono.error(new ResponseStatusException(SERVICE_UNAVAILABLE, 
                                                    "Kafka is unavailable: " + kafkaEx.getMessage(), kafkaEx));
                                        }
                                        // Check for Kafka connection errors
                                        String errorMessage = throwable.getMessage();
                                        if (errorMessage != null && (errorMessage.contains("Kafka") || 
                                                                     errorMessage.contains("broker") || 
                                                                     errorMessage.contains("connection") ||
                                                                     errorMessage.contains("timeout"))) {
                                            // Record delivery failure in ledger (fire and forget)
                                            idempotencyLedgerService.recordEventStatus(
                                                    eventId,
                                                    IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                                                    schemaId
                                            ).subscribe(
                                                    null,
                                                    error -> log.error("Failed to record delivery failure in ledger", error)
                                            );
                                            return Mono.error(new ResponseStatusException(SERVICE_UNAVAILABLE, 
                                                    "Kafka is unavailable: " + errorMessage, throwable));
                                        }
                                        // Record processing failure for non-Kafka errors (fire and forget)
                                        idempotencyLedgerService.recordEventStatus(
                                                eventId,
                                                IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                                schemaId
                                        ).subscribe(
                                                null,
                                                error -> log.error("Failed to record processing failure in ledger", error)
                                        );
                                        // Re-throw if not Kafka-related
                                        return Mono.error(throwable);
                                    });
                        })
                        .onErrorResume(throwable -> {
                            // Record processing failure for schema validation errors (fire and forget)
                            if (throwable instanceof ResponseStatusException) {
                                ResponseStatusException rse = (ResponseStatusException) throwable;
                                // Only record for non-404 errors (404 means schema not found, which is a different case)
                                if (rse.getStatusCode().value() != NOT_FOUND.value()) {
                                    idempotencyLedgerService.recordEventStatus(
                                            eventId,
                                            IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                            schemaId
                                    ).subscribe(
                                            null,
                                            error -> log.error("Failed to record processing failure in ledger", error)
                                    );
                                }
                                return Mono.error(throwable);
                            }
                            return Mono.error(throwable);
                        }))
                .map(id -> {
                    AckResponse response = new AckResponse();
                    response.setEventId(UUID.fromString(id));
                    return ResponseEntity.accepted().body(response);
                })
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
        // Check if validation is enabled
        boolean validationEnabled = properties.validation() != null && properties.validation().isEnabled();
        Mono<JsonNode> validatedJsonMono;
        
        if (!validationEnabled) {
            log.warn("Schema validation is DISABLED (webhooks.validation.enabled=false). Skipping validation for testing purposes.");
            validatedJsonMono = Mono.just(jsonNode);
        } else {
            validatedJsonMono = jsonSchemaValidator.validate(jsonNode, schemaDefinition);
        }
        
        return validatedJsonMono
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
        // Check if validation is enabled
        boolean validationEnabled = properties.validation() != null && properties.validation().isEnabled();
        Mono<JsonNode> validatedJsonMono;
        
        if (!validationEnabled) {
            log.warn("Schema validation is DISABLED (webhooks.validation.enabled=false). Skipping validation for testing purposes.");
            validatedJsonMono = Mono.just(jsonNode);
        } else {
            validatedJsonMono = jsonSchemaValidator.validate(jsonNode, schemaDefinition);
        }
        
        return validatedJsonMono
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
        // Check if validation is enabled
        boolean validationEnabled = properties.validation() != null && properties.validation().isEnabled();
        
        // Parse Avro schema for type coercion (needed even if validation is disabled for serialization)
        Schema avroSchema = new Schema.Parser().parse(avroSchemaString);

        // Coerce JSON types to match Avro schema
        JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);

        // Convert to Avro and validate against Avro schema (or skip validation if disabled)
        Mono<GenericRecord> avroRecordMono;
        if (!validationEnabled) {
            log.warn("Schema validation is DISABLED (webhooks.validation.enabled=false). Skipping Avro validation for testing purposes.");
            // Create a minimal GenericRecord without validation
            avroRecordMono = convertJsonToAvroRecord(coercedJson, avroSchema);
        } else {
            avroRecordMono = avroSchemaValidator.validate(coercedJson, schemaDefinition);
        }
        
        return avroRecordMono
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
        // Check if validation is enabled
        boolean validationEnabled = properties.validation() != null && properties.validation().isEnabled();
        
        String avroSchemaString = schemaDefinition.avroSchema();

        // Validate Avro schema exists (only if validation is enabled)
        if (validationEnabled && (avroSchemaString == null || avroSchemaString.isEmpty())) {
            return Mono.error(new ResponseStatusException(BAD_REQUEST, "Avro schema definition not configured for this event"));
        }

        // Parse Avro schema for type coercion (needed even if validation is disabled for serialization)
        if (avroSchemaString == null || avroSchemaString.isEmpty()) {
            return Mono.error(new ResponseStatusException(BAD_REQUEST, "Avro schema is required for serialization even when validation is disabled"));
        }
        
        // Parse schema once and make it final for use in lambda
        final Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
        // Coerce JSON types to match Avro schema (important for XML where all values are strings)
        final JsonNode coercedJson = coerceJsonToAvroTypes(jsonNode, avroSchema);

        // Convert to Avro and validate against Avro schema (or skip validation if disabled)
        Mono<GenericRecord> avroRecordMono;
        if (!validationEnabled) {
            log.warn("Schema validation is DISABLED (webhooks.validation.enabled=false). Skipping Avro validation for testing purposes.");
            // Create a minimal GenericRecord without validation
            avroRecordMono = convertJsonToAvroRecord(coercedJson, avroSchema);
        } else {
            avroRecordMono = avroSchemaValidator.validate(coercedJson, schemaDefinition);
        }
        
        return avroRecordMono
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
                detail.updateTs(),
                detail.eventSchemaId()
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

    @Override
    public Mono<ResponseEntity<SchemaMetadata>> fetchSchema(String domain, String event, String version, ServerWebExchange exchange) {
        return schemaService.fetchSchema(new SchemaReference(domain, event, version))
                .map(this::convertToSchemaMetadata)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found")))
                .onErrorMap(DynamoDbException.class, e -> {
                    if (e.getMessage().contains("does not exist")) {
                        return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                    }
                    return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    @Override
    public Mono<ResponseEntity<Flux<SchemaMetadata>>> getAllSchemas(ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.ok(
                schemaService.fetchAllSchemas()
                        .map(this::convertToSchemaMetadata)
                        .onErrorMap(DynamoDbException.class, e -> {
                            if (e.getMessage().contains("does not exist")) {
                                return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                            }
                            return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                        })
        ));
    }

    @Override
    public Mono<ResponseEntity<com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse>> getSchemaBySchemaId(String schemaId, ServerWebExchange exchange) {
        return schemaService.fetchSchemaBySchemaId(schemaId)
                .map(this::convertToApiSchemaDetailResponse)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found for schemaId: " + schemaId)))
                .onErrorMap(DynamoDbException.class, e -> {
                    if (e.getMessage().contains("does not exist")) {
                        return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                    }
                    return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    private SchemaMetadata convertToSchemaMetadata(SchemaDefinition schemaDefinition) {
        SchemaMetadata metadata = new SchemaMetadata();
        String schemaId = String.format("SCHEMA_%s_%s_%s",
                schemaDefinition.reference().domain().toUpperCase(),
                schemaDefinition.reference().eventName().toUpperCase(),
                schemaDefinition.reference().version().toUpperCase());
        metadata.setSchemaId(schemaId);
        
        // Set eventSchemaId from DynamoDB if available
        if (schemaDefinition.eventSchemaId() != null && !schemaDefinition.eventSchemaId().isEmpty()) {
            metadata.setEventSchemaId(org.openapitools.jackson.nullable.JsonNullable.of(schemaDefinition.eventSchemaId()));
        }

        SchemaMetadata.FormatTypeEnum formatType = schemaDefinition.formatType() == SchemaFormatType.JSON_SCHEMA
                ? SchemaMetadata.FormatTypeEnum.JSON_SCHEMA
                : SchemaMetadata.FormatTypeEnum.AVRO_SCHEMA;
        metadata.setFormatType(formatType);

        metadata.setStatus(schemaDefinition.active()
                ? SchemaMetadata.StatusEnum.ACTIVE
                : SchemaMetadata.StatusEnum.INACTIVE);

        metadata.setEventName(schemaDefinition.reference().eventName());
        metadata.setProducerDomain(schemaDefinition.reference().domain());
        metadata.setVersion(schemaDefinition.reference().version());

        if (schemaDefinition.jsonSchema() != null && !schemaDefinition.jsonSchema().isEmpty()) {
            metadata.setJsonSchema(org.openapitools.jackson.nullable.JsonNullable.of(schemaDefinition.jsonSchema()));
        }
        if (schemaDefinition.avroSchema() != null && !schemaDefinition.avroSchema().isEmpty()) {
            metadata.setAvroSchema(org.openapitools.jackson.nullable.JsonNullable.of(schemaDefinition.avroSchema()));
        }

        // Set eventSchemaDefinition - contains the primary schema definition from EVENT_SCHEMA_DEFINITION
        // For JSON_SCHEMA format, use jsonSchema; for AVRO_SCHEMA format, use avroSchema
        if (schemaDefinition.formatType() == SchemaFormatType.JSON_SCHEMA 
                && schemaDefinition.jsonSchema() != null && !schemaDefinition.jsonSchema().isEmpty()) {
            metadata.setEventSchemaDefinition(org.openapitools.jackson.nullable.JsonNullable.of(schemaDefinition.jsonSchema()));
        } else if (schemaDefinition.formatType() == SchemaFormatType.AVRO_SCHEMA 
                && schemaDefinition.avroSchema() != null && !schemaDefinition.avroSchema().isEmpty()) {
            metadata.setEventSchemaDefinition(org.openapitools.jackson.nullable.JsonNullable.of(schemaDefinition.avroSchema()));
        }

        if (schemaDefinition.updatedAt() != null) {
            metadata.setUpdatedAt(schemaDefinition.updatedAt().atOffset(java.time.ZoneOffset.UTC));
        }

        return metadata;
    }

    private com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse convertToApiSchemaDetailResponse(
            com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDetailResponse internal) {
        com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse api =
                new com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse();

        api.setEventSchemaId(internal.eventSchemaId());
        api.setProducerDomain(internal.producerDomain());
        api.setEventName(internal.eventName());
        api.setVersion(internal.version());
        api.setEventSchemaHeader(internal.eventSchemaHeader());

        if (internal.eventSchemaDefinition() != null && !internal.eventSchemaDefinition().isEmpty()) {
            api.setEventSchemaDefinition(org.openapitools.jackson.nullable.JsonNullable.of(internal.eventSchemaDefinition()));
        }
        if (internal.eventSchemaDefinitionAvro() != null && !internal.eventSchemaDefinitionAvro().isEmpty()) {
            api.setEventSchemaDefinitionAvro(org.openapitools.jackson.nullable.JsonNullable.of(internal.eventSchemaDefinitionAvro()));
        }

        boolean hasJsonSchema = internal.eventSchemaDefinition() != null && !internal.eventSchemaDefinition().isEmpty();
        com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.FormatTypeEnum formatType = hasJsonSchema
                ? com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.FormatTypeEnum.JSON_SCHEMA
                : com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.FormatTypeEnum.AVRO_SCHEMA;
        api.setFormatType(formatType);

        api.setEventSample(internal.eventSample());

        if (internal.eventSchemaStatus() != null) {
            api.setEventSchemaStatus(
                    "ACTIVE".equals(internal.eventSchemaStatus())
                            ? com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.EventSchemaStatusEnum.ACTIVE
                            : com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.EventSchemaStatusEnum.INACTIVE
            );
        }

        if (internal.hasSensitiveData() != null) {
            api.setHasSensitiveData(
                    "true".equalsIgnoreCase(internal.hasSensitiveData())
                            ? com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.HasSensitiveDataEnum.TRUE
                            : com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.HasSensitiveDataEnum.FALSE
            );
        }

        api.setProducerSystemUsersId(internal.producerSystemUsersId());
        api.setTopicName(internal.topicName());

        if (internal.topicStatus() != null) {
            api.setTopicStatus(
                    "ACTIVE".equals(internal.topicStatus())
                            ? com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.TopicStatusEnum.ACTIVE
                            : com.beewaxus.webhooksvcs.api.model.SchemaDetailResponse.TopicStatusEnum.INACTIVE
            );
        }

        if (internal.insertTs() != null) {
            api.setInsertTs(org.openapitools.jackson.nullable.JsonNullable.of(internal.insertTs().atOffset(java.time.ZoneOffset.UTC)));
        }
        api.setInsertUser(internal.insertUser());

        if (internal.updateTs() != null) {
            api.setUpdateTs(org.openapitools.jackson.nullable.JsonNullable.of(internal.updateTs().atOffset(java.time.ZoneOffset.UTC)));
        }
        api.setUpdateUser(internal.updateUser());

        return api;
    }
}

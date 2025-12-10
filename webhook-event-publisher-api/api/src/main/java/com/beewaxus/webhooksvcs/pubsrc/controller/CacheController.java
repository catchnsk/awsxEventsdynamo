package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.webhooks.api.WebhookApi;
import com.webhooks.model.*;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormatType;
import com.beewaxus.webhooksvcs.pubsrc.schema.DynamoDbException;
import com.beewaxus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.beewaxus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import com.beewaxus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.beewaxus.webhooksvcs.pubsrc.publisher.KafkaPublishException;
import com.beewaxus.webhooksvcs.pubsrc.ledger.IdempotencyLedgerService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import org.springframework.web.server.ServerWebExchange;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.networknt.schema.utils.StringUtils.isBlank;

@RestController
public class CacheController implements WebhookApi {

    private final SchemaService schemaService;
    private final JsonSchemaValidator jsonSchemaValidator;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final WebhooksProperties properties;
    private final IdempotencyLedgerService idempotencyLedgerService;

    public CacheController(SchemaService schemaService,
                           JsonSchemaValidator jsonSchemaValidator,
                           EventPublisher eventPublisher,
                           ObjectMapper objectMapper,
                           WebhooksProperties properties,
                           IdempotencyLedgerService idempotencyLedgerService) {
        this.schemaService = schemaService;
        this.jsonSchemaValidator = jsonSchemaValidator;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.idempotencyLedgerService = idempotencyLedgerService;
    }

    @Override
    public Mono<ResponseEntity<CacheEvictResponseData>> evictAllCaches(String correlationID,
                                                                       String xB3TraceId,
                                                                       String xB3SpanId,
                                                                       String xChannelId,
                                                                       String xUserId,
                                                                       String contentType,
                                                                       ServerWebExchange exchange) {
        CacheEvictResponseDataPayload payload = new CacheEvictResponseDataPayload()
                .msgStatus("Cache eviction completed");
        CacheEvictResponseData body = new CacheEvictResponseData().data(payload);

        return schemaService.evictAndReload()
                .thenReturn(ResponseEntity.status(HttpStatus.ACCEPTED).body(body));
    }

    @Override
    public Mono<ResponseEntity<EventSchemaResponseData>> fetchSchema(String correlationID,
                                                                     String xB3TraceId,
                                                                     String xB3SpanId,
                                                                     String xChannelId,
                                                                     String xUserId,
                                                                     String contentType,
                                                                     Mono<EventPublisherRequestData> eventPublisherRequestData,
                                                                     ServerWebExchange exchange) {
        return eventPublisherRequestData
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Request body is required")))
                .flatMap(body -> {
                    EventPublisherPayload2 data = body.getData();
                    if (data == null) {
                        return Mono.error(new IllegalArgumentException("data is required"));
                    }
                    String domain = data.getDomain();
                    String event = data.getEventName();
                    String version = data.getVersion();
                    if (domain == null || domain.isEmpty()) {
                        return Mono.error(new IllegalArgumentException("domain is required"));
                    }
                    if (event == null || event.isEmpty()) {
                        return Mono.error(new IllegalArgumentException("eventName is required"));
                    }
                    if (version == null || version.isEmpty()) {
                        return Mono.error(new IllegalArgumentException("version is required"));
                    }

                    SchemaReference reference = new SchemaReference(domain, event, version);
                    return schemaService.fetchSchema(reference)
                            .map(this::toSchemaPayload)
                            .map(payload -> {
                                @SuppressWarnings("unchecked")
                                ResponseEntity<EventSchemaResponseData> response =
                                        (ResponseEntity<EventSchemaResponseData>) (ResponseEntity<?>) ResponseEntity.ok(payload);
                                return response;
                            })
                            .switchIfEmpty(Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).build()))
                            .onErrorResume(DynamoDbException.class, e -> {
                                if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                                }
                                return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build());
                            });
                })
                .onErrorResume(IllegalArgumentException.class, e ->
                        Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST).build()));
    }

    @Override
    public Mono<ResponseEntity<EventSchemaResponseData>> getAllSchemas(String correlationID,
                                                                       String xB3TraceId,
                                                                       String xB3SpanId,
                                                                       String xChannelId,
                                                                       String xUserId,
                                                                       String contentType,
                                                                       ServerWebExchange exchange) {
        return schemaService.fetchAllSchemas()
                .map(this::toSchemaPayload)
                .collectList()
                .map(payloads -> {
                    @SuppressWarnings("unchecked")
                    ResponseEntity<EventSchemaResponseData> response =
                            (ResponseEntity<EventSchemaResponseData>) (ResponseEntity<?>) ResponseEntity.ok(payloads);
                    return response;
                })
                .onErrorResume(DynamoDbException.class, e -> {
                    if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                    }
                    return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build());
                });
    }

    

    @Override
    public Mono<ResponseEntity<EventPublisherResponseData>> publishCloudEvent(String correlationID,
                                                                              String xB3TraceId,
                                                                              String xB3SpanId,
                                                                              String xChannelId,
                                                                              String xUserId,
                                                                              String contentType,
                                                                              UUID xEventId,
                                                                              String idempotencyKey,
                                                                              Mono<EventPublishCERequestData> eventPublishCERequestData,
                                                                              ServerWebExchange exchange) {
        return eventPublishCERequestData
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Body required")))
                .flatMap(body -> {
                    EventPublishCEPayload payload = body.getData();
                    if (payload == null) {
                        return Mono.error(new IllegalArgumentException("data is required"));
                    }

                    // Determine IDs and required fields
                    String eventId = xEventId != null ? xEventId.toString() :
                            payload.getId() != null ? payload.getId().toString() : UUID.randomUUID().toString();
                    String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;

                    if (isBlank(payload.getSource())) {
                        return Mono.error(new IllegalArgumentException("source is required"));
                    }
                    if (isBlank(payload.getSubject())) {
                        return Mono.error(new IllegalArgumentException("subject is required"));
                    }
                    if (isBlank(payload.getSpecVersion())) {
                        return Mono.error(new IllegalArgumentException("specVersion is required"));
                    }
                    if (payload.getData() == null || payload.getData().isEmpty()) {
                        return Mono.error(new IllegalArgumentException("data is required"));
                    }

                    SchemaReference reference = new SchemaReference(
                            payload.getSource(),
                            payload.getSubject(),
                            payload.getSpecVersion()
                    );

                    return schemaService.fetchSchema(reference)
                            .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,
                                    "Schema not found for source=%s, subject=%s, specVersion=%s"
                                            .formatted(payload.getSource(), payload.getSubject(), payload.getSpecVersion()))))
                            .onErrorMap(DynamoDbException.class, e -> {
                                if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                                    return new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                                            "DynamoDB table not found: " + e.getMessage(), e);
                                }
                                return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                                        "DynamoDB service unavailable: " + e.getMessage(), e);
                            })
                            .flatMap(schemaDefinition -> {
                                boolean validationEnabled = properties.validation() == null || properties.validation().isEnabled();

                                if (validationEnabled) {
                                    String jsonSchemaStr = schemaDefinition.jsonSchema();
                                    if (isBlank(jsonSchemaStr)) {
                                        return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                                                "JSON Schema (EVENT_SCHEMA_DEFINITION) not configured for this event"));
                                    }
                                    if (jsonSchemaStr.trim().startsWith("{\"type\":\"record\"")) {
                                        return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                                                "EVENT_SCHEMA_DEFINITION contains an Avro schema; use EVENT_SCHEMA_DEFINITION_AVRO for Avro"));
                                    }
                                }

                                JsonNode dataJson = objectMapper.valueToTree(payload.getData());
                                Mono<JsonNode> validated = validationEnabled
                                        ? jsonSchemaValidator.validate(dataJson, schemaDefinition)
                                        : Mono.just(dataJson);

                                String topicName = "%s.%s.%s".formatted(
                                        properties.kafka().ingressTopicPrefix(),
                                        reference.domain(),
                                        reference.eventName()
                                );

                                return validated
                                        .flatMap(validatedJson -> {
                                            EventEnvelope envelope = new EventEnvelope(
                                                    eventId,
                                                    reference,
                                                    objectMapper.valueToTree(Map.of(
                                                            "type", payload.getType() != null ? payload.getType() : "",
                                                            "source", payload.getSource(),
                                                            "subject", payload.getSubject(),
                                                            "specVersion", payload.getSpecVersion(),
                                                            "time", payload.getTime() != null ? payload.getTime().toString() : ""
                                                    )),
                                                    payload.getTime() != null ? payload.getTime().toInstant() : java.time.Instant.now(),
                                                    Map.of(
                                                            "Idempotency-Key", idempotencyKeyValue,
                                                            "Event-Type", payload.getType() != null ? payload.getType() : "",
                                                            "Source", payload.getSource()
                                                    ),
                                                    SchemaFormatType.JSON_SCHEMA
                                            );

                                            return eventPublisher.publishJson(envelope, topicName, validatedJson)
                                                    .flatMap(publishedEventId -> {
                                                        String schemaId = String.format("SCHEMA_%s_%s_%s",
                                                                reference.domain().toUpperCase(),
                                                                reference.eventName().toUpperCase(),
                                                                reference.version().toUpperCase().replace(".", "_"));
                                                        idempotencyLedgerService.recordEventStatus(
                                                                        publishedEventId,
                                                                        IdempotencyLedgerService.EventStatus.EVENT_READY_FOR_DELIVERY,
                                                                        schemaId
                                                                )
                                                                .subscribe();

                                                        EventPublisherResponseDataPayload respPayload = new EventPublisherResponseDataPayload()
                                                                .eventId(UUID.fromString(publishedEventId));
                                                        EventPublisherResponseData resp = new EventPublisherResponseData().data(respPayload);
                                                        return Mono.just(ResponseEntity.status(HttpStatus.ACCEPTED).body(resp));
                                                    })
                                                    .onErrorResume(KafkaPublishException.class, e -> {
                                                        idempotencyLedgerService.recordEventStatus(
                                                                eventId,
                                                                IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                                                                null
                                                        ).subscribe();
                                                        return Mono.error(new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                                                                "Kafka is unavailable: " + e.getMessage(), e));
                                                    });
                                        })
                                        .onErrorResume(throwable -> {
                                            if (throwable instanceof ResponseStatusException) {
                                                return Mono.error(throwable);
                                            }
                                            idempotencyLedgerService.recordEventStatus(
                                                    eventId,
                                                    IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                                                    null
                                            ).subscribe();
                                            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                                                    throwable.getMessage() != null ? throwable.getMessage() : "Validation failed",
                                                    throwable));
                                        });
                            });
                })
                .onErrorResume(IllegalArgumentException.class, e ->
                        Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST).build()));
    }

    @Override
    public Mono<ResponseEntity<EventPublisherResponseData>> publishEvent(String correlationID,
                                                                         String xB3TraceId,
                                                                         String xB3SpanId,
                                                                         String xChannelId,
                                                                         String xUserId,
                                                                         String contentType,
                                                                         UUID xEventId,
                                                                         String idempotencyKey,
                                                                         Mono<EventPublishRequestData> eventPublishRequestData,
                                                                         ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build());
    }

    @Override
    public Mono<ResponseEntity<EventPublisherResponseData>> publisherEvent(String correlationID,
                                                                           String xB3TraceId,
                                                                           String xB3SpanId,
                                                                           String xChannelId,
                                                                           String xUserId,
                                                                           String contentType,
                                                                           UUID xEventId,
                                                                           String idempotencyKey,
                                                                           Mono<EventPublisherRequestData> eventPublisherRequestData,
                                                                           ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build());
    }

    private EventSchemaResponseDataPayload toSchemaPayload(SchemaDefinition definition) {
        SchemaFormatType formatType = definition.formatType();
        String schemaId = String.format(
                "SCHEMA_%s_%s_%s",
                definition.reference().domain().toUpperCase(),
                definition.reference().eventName().toUpperCase(),
                definition.reference().version().toUpperCase().replace(".", "_")
        );

        return new EventSchemaResponseDataPayload()
                .schemaId(schemaId)
                .domain(definition.reference().domain())
                .eventName(definition.reference().eventName())
                .version(definition.reference().version())
                .formatType(formatType != null ? formatType.name() : null)
                .eventSchemaDefinition(definition.jsonSchema());
    }
}

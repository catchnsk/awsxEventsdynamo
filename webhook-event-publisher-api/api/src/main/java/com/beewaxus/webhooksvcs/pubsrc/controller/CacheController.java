package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.webhooks.api.WebhookApi;
import com.webhooks.model.CacheEvictResponseData;
import com.webhooks.model.CacheEvictResponseDataPayload;
import com.webhooks.model.EventPublishCEPayload;
import com.webhooks.model.EventPublishCERequestData;
import com.webhooks.model.EventPublishRequestData;
import com.webhooks.model.EventPublisherPayload2;
import com.webhooks.model.EventPublisherRequestData;
import com.webhooks.model.EventPublisherResponseData;
import com.webhooks.model.EventPublisherResponseDataPayload;
import com.webhooks.model.EventSchemaResponseData;
import com.webhooks.model.EventSchemaResponseDataPayload;
import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.beewaxus.webhooksvcs.pubsrc.ledger.IdempotencyLedgerService;
import com.beewaxus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.beewaxus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.beewaxus.webhooksvcs.pubsrc.publisher.KafkaPublishException;
import com.beewaxus.webhooksvcs.pubsrc.schema.DynamoDbException;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormatType;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import com.beewaxus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

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
    public ResponseEntity<CacheEvictResponseData> evictAllCaches(String correlationID,
                                                                 String xB3TraceId,
                                                                 String xB3SpanId,
                                                                 String xChannelId,
                                                                 String xUserId,
                                                                 String contentType) {
        CacheEvictResponseDataPayload payload = new CacheEvictResponseDataPayload()
                .msgStatus("Cache eviction completed");
        CacheEvictResponseData body = new CacheEvictResponseData().data(payload);
        try {
            if (schemaService.evictAndReload() != null) {
                schemaService.evictAndReload().block();
            }
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
        } catch (DynamoDbException e) {
            HttpStatus status = e.getMessage() != null && e.getMessage().contains("does not exist")
                    ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.SERVICE_UNAVAILABLE;
            return ResponseEntity.status(status).build();
        }
    }

    @Override
    public ResponseEntity<EventSchemaResponseData> fetchSchema(String correlationID,
                                                               String xB3TraceId,
                                                               String xB3SpanId,
                                                               String xChannelId,
                                                               String xUserId,
                                                               String contentType,
                                                               EventPublisherRequestData eventPublisherRequestData) {
        try {
            if (eventPublisherRequestData == null || eventPublisherRequestData.getData() == null) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            EventPublisherPayload2 data = eventPublisherRequestData.getData();
            if (isBlank(data.getDomain()) || isBlank(data.getEventName()) || isBlank(data.getVersion())) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            SchemaReference reference = new SchemaReference(data.getDomain(), data.getEventName(), data.getVersion());
            SchemaDefinition definition = schemaService.fetchSchema(reference).block();
            if (definition == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            EventSchemaResponseDataPayload payload = toSchemaPayload(definition);
            EventSchemaResponseData body = new EventSchemaResponseData().data(payload);
            return ResponseEntity.ok(body);
        } catch (DynamoDbException e) {
            HttpStatus status = e.getMessage() != null && e.getMessage().contains("does not exist")
                    ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.SERVICE_UNAVAILABLE;
            return ResponseEntity.status(status).build();
        }
    }

    @Override
    public ResponseEntity<EventSchemaResponseData> getAllSchemas(String correlationID,
                                                                 String xB3TraceId,
                                                                 String xB3SpanId,
                                                                 String xChannelId,
                                                                 String xUserId,
                                                                 String contentType) {
        try {
            List<EventSchemaResponseDataPayload> payloads = schemaService.fetchAllSchemas()
                    .map(this::toSchemaPayload)
                    .collectList()
                    .block();

            @SuppressWarnings("unchecked")
            ResponseEntity<EventSchemaResponseData> response =
                    (ResponseEntity<EventSchemaResponseData>) (ResponseEntity<?>) ResponseEntity.ok(payloads);
            return response;
        } catch (DynamoDbException e) {
            HttpStatus status = e.getMessage() != null && e.getMessage().contains("does not exist")
                    ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.SERVICE_UNAVAILABLE;
            return ResponseEntity.status(status).build();
        }
    }

    @Override
    public ResponseEntity<EventPublisherResponseData> publishCloudEvent(String correlationID,
                                                                        String xB3TraceId,
                                                                        String xB3SpanId,
                                                                        String xChannelId,
                                                                        String xUserId,
                                                                        String contentType,
                                                                        UUID xEventId,
                                                                        String idempotencyKey,
                                                                        EventPublishCERequestData eventPublishCERequestData) {
        try {
            if (eventPublishCERequestData == null || eventPublishCERequestData.getData() == null) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            EventPublishCEPayload payload = eventPublishCERequestData.getData();

            String eventId = xEventId != null ? xEventId.toString() :
                    payload.getId() != null ? payload.getId().toString() : UUID.randomUUID().toString();
            String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;

            if (isBlank(payload.getSource()) || isBlank(payload.getSubject()) || isBlank(payload.getSpecVersion())) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }
            if (payload.getData() == null || payload.getData().isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }

            SchemaReference reference = new SchemaReference(
                    payload.getSource(),
                    payload.getSubject(),
                    payload.getSpecVersion()
            );

            SchemaDefinition schemaDefinition = schemaService.fetchSchema(reference).block();
            if (schemaDefinition == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }

            boolean validationEnabled = properties.validation() == null || properties.validation().isEnabled();
            if (validationEnabled) {
                String jsonSchemaStr = schemaDefinition.jsonSchema();
                if (isBlank(jsonSchemaStr)) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
                }
                if (jsonSchemaStr.trim().startsWith("{\"type\":\"record\"")) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
                }
            }

            JsonNode dataJson = objectMapper.valueToTree(payload.getData());
            JsonNode validatedJson = validationEnabled
                    ? jsonSchemaValidator.validate(dataJson, schemaDefinition).block()
                    : dataJson;

            String topicName = "%s.%s.%s".formatted(
                    properties.kafka().ingressTopicPrefix(),
                    reference.domain(),
                    reference.eventName()
            );

            String publishedEventId;
            try {
                publishedEventId = eventPublisher.publishJson(new EventEnvelope(
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
                ), topicName, validatedJson).block();
            } catch (KafkaPublishException e) {
                idempotencyLedgerService.recordEventStatus(
                        eventId,
                        IdempotencyLedgerService.EventStatus.EVENT_DELIVERY_FAILED,
                        null
                ).blockOptional();
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
            }

            if (publishedEventId != null) {
                String schemaId = String.format("SCHEMA_%s_%s_%s",
                        reference.domain().toUpperCase(),
                        reference.eventName().toUpperCase(),
                        reference.version().toUpperCase().replace(".", "_"));
                idempotencyLedgerService.recordEventStatus(
                        publishedEventId,
                        IdempotencyLedgerService.EventStatus.EVENT_READY_FOR_DELIVERY,
                        schemaId
                ).blockOptional();
            }

            String responseId = publishedEventId != null ? publishedEventId : eventId;
            EventPublisherResponseDataPayload respPayload = new EventPublisherResponseDataPayload()
                    .eventId(UUID.fromString(responseId));
            EventPublisherResponseData resp = new EventPublisherResponseData().data(respPayload);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(resp);
        } catch (DynamoDbException e) {
            HttpStatus status = e.getMessage() != null && e.getMessage().contains("does not exist")
                    ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.SERVICE_UNAVAILABLE;
            return ResponseEntity.status(status).build();
        } catch (Exception e) {
            idempotencyLedgerService.recordEventStatus(
                    xEventId != null ? xEventId.toString() : UUID.randomUUID().toString(),
                    IdempotencyLedgerService.EventStatus.EVENT_PROCESSING_FAILED,
                    null
            ).blockOptional();
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }

    @Override
    public ResponseEntity<EventPublisherResponseData> publishEvent(String correlationID,
                                                                   String xB3TraceId,
                                                                   String xB3SpanId,
                                                                   String xChannelId,
                                                                   String xUserId,
                                                                   String contentType,
                                                                   UUID xEventId,
                                                                   String idempotencyKey,
                                                                   EventPublishRequestData eventPublishRequestData) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
    }

    @Override
    public ResponseEntity<EventPublisherResponseData> publisherEvent(String correlationID,
                                                                     String xB3TraceId,
                                                                     String xB3SpanId,
                                                                     String xChannelId,
                                                                     String xUserId,
                                                                     String contentType,
                                                                     UUID xEventId,
                                                                     String idempotencyKey,
                                                                     EventPublisherRequestData eventPublisherRequestData) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
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

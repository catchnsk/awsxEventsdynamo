package com.java.barclaycardus.webhooksvcs.pubsrc.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.barclaycardus.webhooksvcs.pubsrc.converter.FormatConverter;
import com.java.barclaycardus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.java.barclaycardus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaFormat;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaService;
import com.java.barclaycardus.webhooksvcs.pubsrc.validation.AvroSchemaValidator;
import com.java.barclaycardus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import com.java.barclaycardus.webhooksvcs.pubsrc.validation.XmlSchemaValidator;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@Validated
public class EventController {

    private final SchemaService schemaService;
    private final JsonSchemaValidator jsonSchemaValidator;
    private final XmlSchemaValidator xmlSchemaValidator;
    private final AvroSchemaValidator avroSchemaValidator;
    private final FormatConverter formatConverter;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    public EventController(SchemaService schemaService,
                           JsonSchemaValidator jsonSchemaValidator,
                           XmlSchemaValidator xmlSchemaValidator,
                           AvroSchemaValidator avroSchemaValidator,
                           FormatConverter formatConverter,
                           EventPublisher eventPublisher,
                           ObjectMapper objectMapper) {
        this.schemaService = schemaService;
        this.jsonSchemaValidator = jsonSchemaValidator;
        this.xmlSchemaValidator = xmlSchemaValidator;
        this.avroSchemaValidator = avroSchemaValidator;
        this.formatConverter = formatConverter;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
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
                        .flatMap(schemaDefinition -> validateAndConvert(payload, format, schemaDefinition))
                        .flatMap(jsonPayload -> eventPublisher.publish(new EventEnvelope(
                                eventId,
                                reference,
                                jsonPayload,
                                Instant.now(),
                                Map.of("Idempotency-Key", idempotencyKeyValue, "Original-Format", format.name())))))
                .map(id -> ResponseEntity.accepted().body(new EventAcceptedResponse(id)));
    }

    private Mono<JsonNode> validateAndConvert(String payload, SchemaFormat format, SchemaDefinition schemaDefinition) {
        return switch (format) {
            case JSON -> Mono.fromCallable(() -> objectMapper.readTree(payload))
                    .flatMap(jsonNode -> jsonSchemaValidator.validate(jsonNode, schemaDefinition)
                            .thenReturn(jsonNode));
            case XML -> xmlSchemaValidator.validate(payload, schemaDefinition)
                    .then(formatConverter.xmlToJson(payload));
            case AVRO -> Mono.fromCallable(() -> objectMapper.readTree(payload))
                    .flatMap(jsonNode -> avroSchemaValidator.validate(jsonNode, schemaDefinition)
                            .thenReturn(jsonNode));
        };
    }

    public record EventAcceptedResponse(String eventId) {}
}

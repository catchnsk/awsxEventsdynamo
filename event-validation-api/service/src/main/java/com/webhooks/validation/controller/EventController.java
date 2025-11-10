package com.webhooks.validation.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.webhooks.validation.model.EventEnvelope;
import com.webhooks.validation.publisher.EventPublisher;
import com.webhooks.validation.schema.SchemaReference;
import com.webhooks.validation.schema.SchemaService;
import com.webhooks.validation.validation.JsonSchemaValidator;
import jakarta.validation.constraints.NotBlank;
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
    private final JsonSchemaValidator schemaValidator;
    private final EventPublisher eventPublisher;

    public EventController(SchemaService schemaService,
                           JsonSchemaValidator schemaValidator,
                           EventPublisher eventPublisher) {
        this.schemaService = schemaService;
        this.schemaValidator = schemaValidator;
        this.eventPublisher = eventPublisher;
    }

    @PostMapping("/events/{eventName}")
    public Mono<ResponseEntity<EventAcceptedResponse>> publishEvent(
            @PathVariable @NotBlank String eventName,
            @RequestHeader("X-Producer-Domain") String domain,
            @RequestHeader("X-Event-Version") String version,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody Mono<JsonNode> payloadMono,
            @RequestHeader Map<String, String> headers) {

        SchemaReference reference = new SchemaReference(domain, eventName, version);
        String eventId = headers.getOrDefault("X-Event-Id", UUID.randomUUID().toString());
        String idempotencyKeyValue = idempotencyKey != null ? idempotencyKey : eventId;

        return payloadMono.switchIfEmpty(Mono.error(new ResponseStatusException(BAD_REQUEST, "Body required")))
                .flatMap(payload -> schemaService.fetchSchema(reference)
                        .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found")))
                        .flatMap(schemaDefinition -> schemaValidator.validate(payload, schemaDefinition)
                                .then(eventPublisher.publish(new EventEnvelope(
                                        eventId,
                                        reference,
                                        payload,
                                        Instant.now(),
                                        Map.of("Idempotency-Key", idempotencyKeyValue)))))
                .map(id -> ResponseEntity.accepted().body(new EventAcceptedResponse(id)));
    }

    public record EventAcceptedResponse(String eventId) {}
}

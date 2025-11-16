package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.beewaxus.webhooksvcs.pubsrc.schema.DynamoDbException;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDetailResponse;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@RestController
public class SchemaController {

    private final SchemaService schemaService;

    public SchemaController(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @GetMapping("/schemas/{domain}/{event}/{version}")
    public Mono<ResponseEntity<SchemaDefinition>> getSchema(@PathVariable String domain,
                                                            @PathVariable String event,
                                                            @PathVariable String version) {
        return schemaService.fetchSchema(new SchemaReference(domain, event, version))
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found")))
                .onErrorMap(DynamoDbException.class, e -> {
                    if (e.getMessage().contains("does not exist")) {
                        return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                    }
                    return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    @GetMapping("/events/all")
    public Mono<ResponseEntity<List<SchemaDefinition>>> getAllSchemas() {
        return schemaService.fetchAllSchemas()
                .collectList()
                .map(ResponseEntity::ok)
                .onErrorMap(DynamoDbException.class, e -> {
                    if (e.getMessage().contains("does not exist")) {
                        return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                    }
                    return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    @GetMapping("/events/schema_id/{schemaId}")
    public Mono<ResponseEntity<SchemaDetailResponse>> getSchemaBySchemaId(@PathVariable String schemaId) {
        return schemaService.fetchSchemaBySchemaId(schemaId)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found for schemaId: " + schemaId)))
                .onErrorMap(DynamoDbException.class, e -> {
                    if (e.getMessage().contains("does not exist")) {
                        return new ResponseStatusException(INTERNAL_SERVER_ERROR, "DynamoDB table not found: " + e.getMessage(), e);
                    }
                    return new ResponseStatusException(SERVICE_UNAVAILABLE, "DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }
}

package com.java.barclaycardus.webhooksvcs.pubsrc.controller;

import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import static org.springframework.http.HttpStatus.NOT_FOUND;

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
                .switchIfEmpty(Mono.error(new ResponseStatusException(NOT_FOUND, "Schema not found")));
    }
}

package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.beewaxus.webhooksvcs.api.CacheManagementApi;
import com.beewaxus.webhooksvcs.api.model.CacheEvictResponse;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
public class CacheController implements CacheManagementApi {

    private final SchemaService schemaService;

    public CacheController(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @Override
    public Mono<ResponseEntity<CacheEvictResponse>> evictAllCaches(ServerWebExchange exchange) {
        return schemaService.evictAndReload()
                .then(Mono.fromCallable(() -> {
                    CacheEvictResponse response = new CacheEvictResponse();
                    response.setStatus("Cache eviction completed");
                    return ResponseEntity.ok(response);
                }));
    }
}

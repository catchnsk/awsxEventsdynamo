package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/cache")
public class CacheController {

    private final SchemaService schemaService;

    public CacheController(SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @PostMapping("/evict/all")
    public Mono<ResponseEntity<CacheEvictResponse>> evictAllCaches() {
        return schemaService.evictAndReload()
                .thenReturn(ResponseEntity.ok(new CacheEvictResponse("Cache eviction completed")));
    }

    public record CacheEvictResponse(String status) {}
}

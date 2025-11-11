package com.barclaycardus.webhooksvcs.evntsrc.schema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.function.Supplier;

@Component
public class SchemaCache {

    private final Cache<String, SchemaMetadata> cache;

    public SchemaCache(com.barclaycardus.webhooksvcs.evntsrc.config.ListenerProperties properties) {
        Duration ttl = properties.getAws().getDynamoDb().getSchemaCacheTtl();
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(1_000)
                .build();
    }

    public SchemaMetadata get(String schemaId, Supplier<SchemaMetadata> loader) {
        return cache.get(schemaId, key -> loader.get());
    }
}

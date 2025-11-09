package com.webhooks.sdk.core;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

final class InMemorySchemaCache implements SchemaCache {

    private final int maxEntries;
    private final Map<SchemaReference, SchemaDefinition> cache;

    InMemorySchemaCache(int maxEntries) {
        this.maxEntries = Math.max(10, maxEntries);
        this.cache = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<SchemaReference, SchemaDefinition> eldest) {
                return size() > InMemorySchemaCache.this.maxEntries;
            }
        };
    }

    @Override
    public synchronized Optional<SchemaDefinition> get(SchemaReference reference) {
        return Optional.ofNullable(cache.get(reference));
    }

    @Override
    public synchronized void put(SchemaDefinition definition) {
        cache.put(definition.reference(), definition);
    }
}

package com.webhooks.sdk.core;

import java.util.Optional;

/**
 * Cache abstraction so SDK can plug in in-memory or external caches.
 */
public interface SchemaCache {

    Optional<SchemaDefinition> get(SchemaReference reference);

    void put(SchemaDefinition definition);

    static SchemaCache inMemory(int maxEntries) {
        return new InMemorySchemaCache(maxEntries);
    }
}

package com.webhooks.sdk.core;

/**
 * Fetches schema definitions from the platform registry (API, DynamoDB, etc).
 */
public interface SchemaRegistryClient {

    SchemaDefinition fetch(SchemaReference reference);

    static SchemaRegistryClient caching(SchemaFetcher fetcher, SchemaCache cache) {
        return new SchemaRegistryClient() {
            @Override
            public SchemaDefinition fetch(SchemaReference reference) {
                return cache.get(reference)
                        .orElseGet(() -> {
                            SchemaDefinition definition = fetcher.fetch(reference);
                            cache.put(definition);
                            return definition;
                        });
            }
        };
    }

    @FunctionalInterface
    interface SchemaFetcher {
        SchemaDefinition fetch(SchemaReference reference);
    }
}

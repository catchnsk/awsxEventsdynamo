package com.webhooks.sdk.core;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents a schema version stored in DynamoDB/S3 registry.
 */
public final class SchemaDefinition {

    private final SchemaReference reference;
    private final String rawSchema;
    private final Instant updatedAt;

    public SchemaDefinition(SchemaReference reference, String rawSchema, Instant updatedAt) {
        this.reference = Objects.requireNonNull(reference, "reference");
        this.rawSchema = Objects.requireNonNull(rawSchema, "rawSchema");
        this.updatedAt = Objects.requireNonNull(updatedAt, "updatedAt");
    }

    public SchemaReference reference() {
        return reference;
    }

    public String rawSchema() {
        return rawSchema;
    }

    public Instant updatedAt() {
        return updatedAt;
    }
}

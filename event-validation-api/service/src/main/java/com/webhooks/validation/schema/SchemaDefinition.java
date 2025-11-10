package com.webhooks.validation.schema;

import java.time.Instant;

public record SchemaDefinition(
        SchemaReference reference,
        String jsonSchema,
        boolean active,
        Instant updatedAt
) {}

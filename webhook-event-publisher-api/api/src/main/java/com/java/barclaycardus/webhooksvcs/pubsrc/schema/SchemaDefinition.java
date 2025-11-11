package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

import java.time.Instant;

public record SchemaDefinition(
        SchemaReference reference,
        String jsonSchema,
        boolean active,
        Instant updatedAt
) {}

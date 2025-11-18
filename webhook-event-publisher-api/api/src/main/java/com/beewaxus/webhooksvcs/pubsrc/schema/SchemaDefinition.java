package com.beewaxus.webhooksvcs.pubsrc.schema;

import java.time.Instant;

public record SchemaDefinition(
        SchemaReference reference,
        String avroSchema,
        boolean active,
        Instant updatedAt
) {}

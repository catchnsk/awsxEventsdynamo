package com.beewaxus.webhooksvcs.pubsrc.schema;

import java.time.Instant;
import java.util.Map;

public record SchemaDefinition(
        SchemaReference reference,
        String jsonSchema,
        String xmlSchema,
        String avroSchema,
        boolean active,
        Instant updatedAt
) {
    public String getSchemaForFormat(SchemaFormat format) {
        return switch (format) {
            case JSON -> jsonSchema;
            case XML -> xmlSchema;
            case AVRO -> avroSchema;
        };
    }
}

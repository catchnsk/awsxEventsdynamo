package com.beewaxus.webhooksvcs.pubsrc.schema;

import java.time.Instant;

public record SchemaDefinition(
        SchemaReference reference,
        String jsonSchema,         // JSON Schema from EVENT_SCHEMA_DEFINITION
        String avroSchema,         // Avro Schema from EVENT_SCHEMA_DEFINITION_AVRO
        SchemaFormatType formatType, // Determines validation and serialization format
        boolean active,
        Instant updatedAt,
        String eventSchemaId       // EVENT_SCHEMA_ID from DynamoDB (optional)
) {
    // Backward compatibility: use Avro schema if JSON schema is not available
    public String getValidationSchema() {
        return formatType == SchemaFormatType.JSON_SCHEMA ? jsonSchema : avroSchema;
    }
}
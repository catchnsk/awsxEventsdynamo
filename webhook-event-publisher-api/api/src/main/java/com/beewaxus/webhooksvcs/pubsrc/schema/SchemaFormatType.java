package com.beewaxus.webhooksvcs.pubsrc.schema;

public enum SchemaFormatType {
    JSON_SCHEMA,  // Validates with JSON Schema, publishes as JSON to Kafka
    AVRO_SCHEMA   // Validates with Avro Schema, publishes as Avro binary to Kafka
}

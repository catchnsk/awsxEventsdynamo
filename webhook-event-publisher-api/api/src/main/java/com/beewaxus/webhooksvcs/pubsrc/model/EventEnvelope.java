package com.beewaxus.webhooksvcs.pubsrc.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormatType;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;

import java.time.Instant;
import java.util.Map;

public record EventEnvelope(
        String eventId,
        SchemaReference schemaReference,
        JsonNode payload,
        Instant timestamp,
        Map<String, String> headers,
       SchemaFormatType schemaFormatType  // Indicates whether payload is validated with JSON Schema or Avro Schema
) {}

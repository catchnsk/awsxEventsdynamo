package com.java.barclaycardus.webhooksvcs.pubsrc.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaReference;

import java.time.Instant;
import java.util.Map;

public record EventEnvelope(
        String eventId,
        SchemaReference schemaReference,
        JsonNode payload,
        Instant timestamp,
        Map<String, String> headers
) {}

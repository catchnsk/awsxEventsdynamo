package com.webhooks.sdk.core;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents an event envelope.
 */
public final class Event {

    private final SchemaReference schema;
    private final String id;
    private final Instant timestamp;
    private final JsonNode payload;
    private final Map<String, String> headers;

    private Event(Builder builder) {
        this.schema = Objects.requireNonNull(builder.schema, "schema");
        this.id = Objects.requireNonNull(builder.id, "id");
        this.timestamp = Objects.requireNonNull(builder.timestamp, "timestamp");
        this.payload = Objects.requireNonNull(builder.payload, "payload");
        this.headers = Map.copyOf(builder.headers);
    }

    public static Builder builder(SchemaReference schemaReference) {
        return new Builder().schema(schemaReference);
    }

    public SchemaReference schema() {
        return schema;
    }

    public String id() {
        return id;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public JsonNode payload() {
        return payload;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public static final class Builder {
        private SchemaReference schema;
        private String id = UUID.randomUUID().toString();
        private Instant timestamp = Instant.now();
        private JsonNode payload;
        private Map<String, String> headers = Map.of();

        public Builder schema(SchemaReference schemaReference) {
            this.schema = schemaReference;
            return this;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder payload(JsonNode payload) {
            this.payload = payload;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Event build() {
            return new Event(this);
        }
    }
}

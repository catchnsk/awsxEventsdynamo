package com.barclaycardus.webhooksvcs.evntsrc.model;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class InboundMessageContext {
    private final String schemaId;
    private final String payload;
    private final Map<String, Object> headers;
    private final String fallbackPartitionKey;
    private final String contentType;

    public InboundMessageContext(String schemaId, String payload, Map<String, Object> headers, String fallbackPartitionKey, String contentType) {
        this.schemaId = schemaId;
        this.payload = payload;
        this.headers = headers == null ? Collections.emptyMap() : Map.copyOf(headers);
        this.fallbackPartitionKey = fallbackPartitionKey;
        this.contentType = contentType;
    }

    public Optional<String> schemaId() {
        return Optional.ofNullable(schemaId);
    }

    public String payload() {
        return payload;
    }

    public Map<String, Object> headers() {
        return headers;
    }

    public Optional<String> fallbackPartitionKey() {
        return Optional.ofNullable(fallbackPartitionKey);
    }

    public String contentType() {
        return contentType;
    }
}

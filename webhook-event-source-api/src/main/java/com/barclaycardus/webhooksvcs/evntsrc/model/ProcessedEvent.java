package com.barclaycardus.webhooksvcs.evntsrc.model;

import com.barclaycardus.webhooksvcs.evntsrc.schema.SchemaMetadata;

public class ProcessedEvent {
    private final SchemaMetadata schema;
    private final byte[] payload;
    private final String partitionKey;

    public ProcessedEvent(SchemaMetadata schema, byte[] payload, String partitionKey) {
        this.schema = schema;
        this.payload = payload;
        this.partitionKey = partitionKey;
    }

    public SchemaMetadata schema() {
        return schema;
    }

    public byte[] payload() {
        return payload;
    }

    public String partitionKey() {
        return partitionKey;
    }
}

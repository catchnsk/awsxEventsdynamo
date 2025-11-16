package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

public record SchemaReference(String domain, String eventName, String version) {

    public String partitionKey() {
        return "SCHEMA#%s#%s".formatted(domain, eventName);
    }

    public String sortKey() {
        return "v%s".formatted(version);
    }
}

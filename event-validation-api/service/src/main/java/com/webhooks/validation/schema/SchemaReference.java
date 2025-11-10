package com.webhooks.validation.schema;

public record SchemaReference(String domain, String eventName, String version) {

    public String partitionKey() {
        return "%s#%s#%s".formatted(domain, eventName, version);
    }
}

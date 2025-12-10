package com.beewaxus.webhooksvcs.pubsrc.schema;

public enum SchemaFormat {
    JSON("application/json"),
    XML("application/xml"),
    AVRO("application/avro");

    private final String contentType;

    SchemaFormat(String contentType) {
        this.contentType = contentType;
    }

    public String getContentType() {
        return contentType;
    }

    public static SchemaFormat fromContentType(String contentType) {
        if (contentType == null) {
            return JSON; // default
        }
        String normalized = contentType.toLowerCase().split(";")[0].trim();
        for (SchemaFormat format : values()) {
            if (format.contentType.equals(normalized)) {
                return format;
            }
        }
        return JSON; // default fallback
    }
}
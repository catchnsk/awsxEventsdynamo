package com.barclaycardus.webhooksvcs.evntsrc.schema;

import java.util.Objects;

public class SchemaMetadata {
    private final String schemaId;
    private final String schemaDefinition;
    private final FormatType formatType;
    private final boolean transformationRequired;
    private final Status status;
    private final String producerDomain;
    private final String eventName;

    public SchemaMetadata(String schemaId, String schemaDefinition, FormatType formatType,
                          boolean transformationRequired, Status status, String producerDomain, String eventName) {
        this.schemaId = schemaId;
        this.schemaDefinition = schemaDefinition;
        this.formatType = formatType;
        this.transformationRequired = transformationRequired;
        this.status = status;
        this.producerDomain = producerDomain;
        this.eventName = eventName;
    }

    public String schemaId() {
        return schemaId;
    }

    public String schemaDefinition() {
        return schemaDefinition;
    }

    public FormatType formatType() {
        return formatType;
    }

    public boolean transformationRequired() {
        return transformationRequired;
    }

    public Status status() {
        return status;
    }

    public String producerDomain() {
        return producerDomain;
    }

    public String eventName() {
        return eventName;
    }

    public boolean isActive() {
        return Status.ACTIVE.equals(status);
    }

    public enum FormatType {
        JSON,
        XML,
        SOAP,
        AVRO
    }

    public enum Status {
        ACTIVE,
        INACTIVE
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "schemaId='" + schemaId + '\'' +
                ", formatType=" + formatType +
                ", transformationRequired=" + transformationRequired +
                ", status=" + status +
                ", producerDomain='" + producerDomain + '\'' +
                ", eventName='" + eventName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaMetadata that = (SchemaMetadata) o;
        return Objects.equals(schemaId, that.schemaId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId);
    }
}

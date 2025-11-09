package com.webhooks.sdk.core;

import java.util.Objects;

/**
 * Identifier for a schema in the registry (producer domain + event + version).
 */
public final class SchemaReference {

    private final String domain;
    private final String eventName;
    private final String version;

    private SchemaReference(String domain, String eventName, String version) {
        this.domain = domain;
        this.eventName = eventName;
        this.version = version;
    }

    public static SchemaReference of(String domain, String eventName, String version) {
        return new SchemaReference(domain, eventName, version);
    }

    public String domain() {
        return domain;
    }

    public String eventName() {
        return eventName;
    }

    public String version() {
        return version;
    }

    public String cacheKey() {
        return domain + "#" + eventName + "#" + version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaReference that = (SchemaReference) o;
        return domain.equals(that.domain) && eventName.equals(that.eventName) && version.equals(that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, eventName, version);
    }

    @Override
    public String toString() {
        return "SchemaReference{" +
                "domain='" + domain + '\'' +
                ", eventName='" + eventName + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}

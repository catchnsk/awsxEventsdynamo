package com.webhooks.sdk.core;

import java.util.Map;
import java.util.Objects;

/**
 * Authentication configuration used by transports.
 */
public final class Auth {

    public enum Type {
        OIDC,
        API_KEY,
        IAM
    }

    private final Type type;
    private final Map<String, String> attributes;

    private Auth(Type type, Map<String, String> attributes) {
        this.type = Objects.requireNonNull(type, "type");
        this.attributes = Map.copyOf(attributes);
    }

    public static Auth oidc(String clientId, String clientSecret) {
        return new Auth(Type.OIDC, Map.of("clientId", clientId, "clientSecret", clientSecret));
    }

    public static Auth apiKey(String key) {
        return new Auth(Type.API_KEY, Map.of("apiKey", key));
    }

    public static Auth iam() {
        return new Auth(Type.IAM, Map.of());
    }

    public Type type() {
        return type;
    }

    public Map<String, String> attributes() {
        return attributes;
    }
}

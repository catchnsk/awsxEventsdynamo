package com.webhooks.sdk.http;

/**
 * Provides an auth token to attach to HTTP calls (OIDC/JWT/API Key etc.).
 */
public interface TokenProvider {
    String getToken();

    static TokenProvider staticToken(String token) {
        return () -> token;
    }
}

package com.webhooks.sdk.http;

public class HttpPublishException extends RuntimeException {
    public HttpPublishException(String message) {
        super(message);
    }

    public HttpPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.webhooks.sdk.kafka;

public class KafkaWebhookPublisherException extends RuntimeException {
    public KafkaWebhookPublisherException(String message, Throwable cause) {
        super(message, cause);
    }
}

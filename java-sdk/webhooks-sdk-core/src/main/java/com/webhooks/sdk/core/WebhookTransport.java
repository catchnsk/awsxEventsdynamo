package com.webhooks.sdk.core;

/**
 * Transport abstraction (HTTP, Kafka, etc.).
 */
public interface WebhookTransport {

    PublishResponse send(Event event);
}

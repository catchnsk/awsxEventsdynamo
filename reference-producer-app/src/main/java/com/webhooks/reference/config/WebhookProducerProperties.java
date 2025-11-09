package com.webhooks.reference.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "producer")
public record WebhookProducerProperties(
        String domain,
        String eventName,
        String schemaVersion,
        String endpoint,
        String token
) {}

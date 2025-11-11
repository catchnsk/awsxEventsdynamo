package com.java.barclaycardus.webhooksvcs.pubsrc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "webhooks")
public record WebhooksProperties(
        DynamoProperties dynamodb,
        KafkaProperties kafka
) {

    public record DynamoProperties(String tableName, Duration cacheTtl) {}

    public record KafkaProperties(String bootstrapServers, String ingressTopicPrefix) {}
}

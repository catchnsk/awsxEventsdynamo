package com.beewaxus.webhooksvcs.pubsrc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "webhooks")
public record WebhooksProperties(
        DynamoProperties dynamodb,
        KafkaProperties kafka
) {

    public record DynamoProperties(String tableName, Duration cacheTtl) {}

    public record KafkaProperties(
            String bootstrapServers,
            String ingressTopicPrefix,
            Duration publishTimeout,
            Integer maxRetries,
            Duration retryBackoffInitialDelay
    ) {
        // Helper methods to get values with defaults (not overriding accessors to avoid recursion)
        public Duration getPublishTimeout() {
            return publishTimeout != null ? publishTimeout : Duration.ofSeconds(30);
        }
        
        public int getMaxRetries() {
            return maxRetries != null ? maxRetries : 2;
        }
        
        public Duration getRetryBackoffInitialDelay() {
            return retryBackoffInitialDelay != null ? retryBackoffInitialDelay : Duration.ofSeconds(1);
        }
    }
}



package com.beewaxus.webhooksvcs.pubsrc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "webhooks")
public record WebhooksProperties(
        DynamoProperties dynamodb,
        KafkaProperties kafka,
        CacheProperties cache,
        ValidationProperties validation
) {

    public record DynamoProperties(
            String tableName,
            String idempotencyLedgerTableName
    ) {
        public String idempotencyLedgerTableName() {
            return idempotencyLedgerTableName != null ? idempotencyLedgerTableName : "EVENT_IDEMPOTENCY_LEDGER";
        }
    }

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

    public record CacheProperties(
            Boolean enabled,
            Duration schemaTtl,
            Duration schemaDetailTtl,
            Integer maximumEntries
    ) {
        public boolean isEnabled() {
            return enabled == null || Boolean.TRUE.equals(enabled);
        }

        public Duration getSchemaTtl() {
            return schemaTtl != null ? schemaTtl : Duration.ofMinutes(5);
        }

        public Duration getSchemaDetailTtl() {
            return schemaDetailTtl != null ? schemaDetailTtl : Duration.ofMinutes(5);
        }

        public         int getMaximumEntries() {
            return maximumEntries != null ? maximumEntries : 1000;
        }
    }

    public record ValidationProperties(
            Boolean enabled
    ) {
        public boolean isEnabled() {
            return enabled == null || Boolean.TRUE.equals(enabled);
        }
    }
}

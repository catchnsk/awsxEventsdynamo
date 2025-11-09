package com.webhooks.sdk.core;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Fluent client that validates payloads and delegates to transports.
 */
public final class WebhookClient {

    private static final Logger log = LoggerFactory.getLogger(WebhookClient.class);

    private final String environment;
    private final Auth auth;
    private final String endpoint;
    private final SchemaRegistryClient schemaRegistryClient;
    private final SchemaValidator schemaValidator;
    private final WebhookTransport transport;
    private final RetryPolicy retryPolicy;
    private final TelemetryReporter telemetry;

    private WebhookClient(Builder builder) {
        this.environment = builder.environment;
        this.auth = builder.auth;
        this.endpoint = builder.endpoint;
        this.schemaRegistryClient = Objects.requireNonNull(builder.resolvedRegistryClient, "schemaRegistryClient");
        this.schemaValidator = Objects.requireNonNull(builder.schemaValidator, "schemaValidator");
        this.transport = Objects.requireNonNull(builder.transport, "transport");
        this.retryPolicy = Objects.requireNonNull(builder.retryPolicy, "retryPolicy");
        this.telemetry = builder.telemetryReporter != null ? builder.telemetryReporter : TelemetryReporter.NOOP;
    }

    public static Builder builder() {
        return new Builder();
    }

    public PublishResponse publish(Event event) {
        Objects.requireNonNull(event, "event");
        SchemaDefinition definition = schemaRegistryClient.fetch(event.schema());
        schemaValidator.validate(event.payload(), definition);

        int attempt = 1;
        while (true) {
            try {
                Instant start = Instant.now();
                PublishResponse response = transport.send(event);
                telemetry.onSuccess(event, response);
                return response;
            } catch (Exception ex) {
                telemetry.onFailure(event, ex, attempt);
                if (attempt >= retryPolicy.maxAttempts()) {
                    throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
                }
                Duration delay = retryPolicy.computeDelay(attempt);
                log.warn("Retrying event {} attempt {}/{} after {}", event.id(), attempt, retryPolicy.maxAttempts(), delay, ex);
                sleep(delay);
                attempt++;
            }
        }
    }

    private static void sleep(Duration delay) {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Builder {
        private String environment = "dev";
        private Auth auth = Auth.apiKey("local");
        private String endpoint = "http://localhost:8080";
        private SchemaCache schemaCache = SchemaCache.inMemory(300);
        private SchemaRegistryClient resolvedRegistryClient;
        private SchemaRegistryClient.SchemaFetcher schemaFetcher = ref -> {
            throw new IllegalStateException("No schema fetcher configured");
        };
        private SchemaValidator schemaValidator = new SchemaValidator();
        private WebhookTransport transport;
        private RetryPolicy retryPolicy = new RetryPolicy(3, Duration.ofMillis(200));
        private TelemetryReporter telemetryReporter;

        public Builder env(String environment) {
            this.environment = environment;
            return this;
        }

        public Builder auth(Auth auth) {
            this.auth = auth;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder schemaCache(SchemaCache schemaCache) {
            this.schemaCache = schemaCache;
            return this;
        }

        public Builder schemaFetcher(SchemaRegistryClient.SchemaFetcher fetcher) {
            this.schemaFetcher = fetcher;
            return this;
        }

        public Builder schemaRegistryClient(SchemaRegistryClient client) {
            this.resolvedRegistryClient = client;
            return this;
        }

        public Builder schemaValidator(SchemaValidator validator) {
            this.schemaValidator = validator;
            return this;
        }

        public Builder transport(WebhookTransport transport) {
            this.transport = transport;
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder telemetryReporter(TelemetryReporter telemetryReporter) {
            this.telemetryReporter = telemetryReporter;
            return this;
        }

        public WebhookClient build() {
            if (resolvedRegistryClient == null) {
                resolvedRegistryClient = SchemaRegistryClient.caching(schemaFetcher, schemaCache);
            }
            Objects.requireNonNull(transport, "transport must be provided (http or kafka module)");
            return new WebhookClient(this);
        }
    }
}

package com.webhooks.sdk.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.sdk.core.Event;
import com.webhooks.sdk.core.PublishResponse;
import com.webhooks.sdk.core.WebhookTransport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * HTTP transport that targets the Event Validation API.
 */
public final class HttpWebhookPublisher implements WebhookTransport {

    private final HttpClient httpClient;
    private final URI endpoint;
    private final String environment;
    private final TokenProvider tokenProvider;
    private final ObjectMapper objectMapper;

    private HttpWebhookPublisher(Builder builder) {
        this.httpClient = builder.httpClient != null ? builder.httpClient : HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.endpoint = URI.create(Objects.requireNonNull(builder.endpoint, "endpoint"));
        this.environment = builder.environment;
        this.tokenProvider = builder.tokenProvider;
        this.objectMapper = builder.objectMapper != null ? builder.objectMapper : new ObjectMapper();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PublishResponse send(Event event) {
        try {
            URI uri = endpoint.resolve("/events/" + event.schema().eventName());
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("X-Producer-Domain", event.schema().domain())
                    .header("X-Event-Version", event.schema().version())
                    .header("X-Env", environment)
                    .header("Idempotency-Key", event.headers().getOrDefault("Idempotency-Key", event.id()));

            if (tokenProvider != null) {
                builder.header("Authorization", "Bearer " + tokenProvider.getToken());
            }

            event.headers().forEach((k, v) -> {
                if (k.startsWith("X-")) {
                    builder.header(k, v);
                }
            });

            builder.POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(event.payload())));
            Instant start = Instant.now();
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 202) {
                Duration latency = Duration.between(start, Instant.now());
                return PublishResponse.accepted(event.id(), Instant.now(), latency);
            }
            throw new HttpPublishException("Platform rejected event " + event.id() + " with status " + response.statusCode());
        } catch (IOException e) {
            throw new HttpPublishException("Failed to send event " + event.id(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpPublishException("Interrupted while sending event " + event.id(), e);
        }
    }

    public static final class Builder {
        private HttpClient httpClient;
        private String endpoint;
        private String environment = "dev";
        private TokenProvider tokenProvider;
        private ObjectMapper objectMapper;

        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder environment(String environment) {
            this.environment = environment;
            return this;
        }

        public Builder tokenProvider(TokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
            return this;
        }

        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public HttpWebhookPublisher build() {
            return new HttpWebhookPublisher(this);
        }
    }
}

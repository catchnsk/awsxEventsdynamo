package com.webhooks.sdk.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.sdk.core.Event;
import com.webhooks.sdk.core.PublishResponse;
import com.webhooks.sdk.core.WebhookTransport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Kafka transport that publishes directly to MSK egress/ingress topics.
 */
public final class KafkaWebhookPublisher implements WebhookTransport, AutoCloseable {

    private final Supplier<KafkaProducer<String, String>> producerSupplier;
    private final String topic;
    private final ObjectMapper objectMapper;

    private KafkaWebhookPublisher(Builder builder) {
        this.producerSupplier = Objects.requireNonNull(builder.producerSupplier, "producerSupplier");
        this.topic = Objects.requireNonNull(builder.topic, "topic");
        this.objectMapper = builder.objectMapper != null ? builder.objectMapper : new ObjectMapper();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PublishResponse send(Event event) {
        try (KafkaProducer<String, String> producer = producerSupplier.get()) {
            String payload = objectMapper.writeValueAsString(event.payload());
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey(event), payload);
            record.headers().add("eventId", event.id().getBytes(StandardCharsets.UTF_8));
            event.headers().forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(5, TimeUnit.SECONDS);
            return PublishResponse.accepted(event.id(), Instant.now(), Duration.ZERO);
        } catch (Exception e) {
            throw new KafkaWebhookPublisherException("Failed to publish to Kafka topic " + topic, e);
        }
    }

    private String partitionKey(Event event) {
        return event.headers().getOrDefault("tenantId", event.id());
    }

    @Override
    public void close() {
        // producers are closed per send to avoid managing lifecycle in SDK
    }

    public static final class Builder {
        private Supplier<KafkaProducer<String, String>> producerSupplier;
        private String topic;
        private ObjectMapper objectMapper;

        public Builder producerSupplier(Supplier<KafkaProducer<String, String>> producerSupplier) {
            this.producerSupplier = producerSupplier;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder objectMapper(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public KafkaWebhookPublisher build() {
            return new KafkaWebhookPublisher(this);
        }
    }
}

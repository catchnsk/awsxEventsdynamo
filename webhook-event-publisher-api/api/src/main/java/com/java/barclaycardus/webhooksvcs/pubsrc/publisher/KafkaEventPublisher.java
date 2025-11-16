package com.java.barclaycardus.webhooksvcs.pubsrc.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.barclaycardus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.java.barclaycardus.webhooksvcs.pubsrc.model.EventEnvelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Component
public class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, byte[]> avroKafkaTemplate;
    private final WebhooksProperties properties;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               KafkaTemplate<String, byte[]> avroKafkaTemplate,
                               WebhooksProperties properties,
                               ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroKafkaTemplate = avroKafkaTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<String> publish(EventEnvelope envelope) {
        Duration timeout = properties.kafka().getPublishTimeout();
        int maxRetries = properties.kafka().getMaxRetries();
        Duration retryDelay = properties.kafka().getRetryBackoffInitialDelay();
        
        return Mono.fromCallable(() -> toJson(envelope))
                .flatMap(payload -> Mono.fromFuture(send(envelope, payload))
                        .timeout(timeout)
                        .retryWhen(Retry.backoff(maxRetries, retryDelay)
                                .filter(throwable -> throwable instanceof org.apache.kafka.common.errors.TimeoutException 
                                        || (throwable.getCause() != null && throwable.getCause() instanceof org.apache.kafka.common.errors.TimeoutException)
                                        || throwable instanceof org.apache.kafka.common.errors.RetriableException))
                        .doOnError(error -> log.error("Failed to publish event {} to Kafka: {}", envelope.eventId(), error.getMessage(), error))
                        .onErrorMap(throwable -> {
                            if (throwable instanceof org.apache.kafka.common.errors.TimeoutException 
                                    || throwable instanceof java.util.concurrent.TimeoutException
                                    || (throwable.getCause() != null && throwable.getCause() instanceof org.apache.kafka.common.errors.TimeoutException)) {
                                return new KafkaPublishException("Kafka publish timeout - broker may be unavailable", throwable);
                            }
                            return new KafkaPublishException("Failed to publish event to Kafka: " + throwable.getMessage(), throwable);
                        }))
                .map(result -> {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.debug("Published event {} to {}-{}@{}", envelope.eventId(), metadata.topic(), metadata.partition(), metadata.offset());
                    return envelope.eventId();
                });
    }

    private CompletableFuture<SendResult<String, String>> send(EventEnvelope envelope, String payload) {
        String topic = "%s.%s.%s".formatted(
                properties.kafka().ingressTopicPrefix(),
                envelope.schemaReference().domain(),
                envelope.schemaReference().eventName());
        return kafkaTemplate.send(topic, envelope.eventId(), payload).toCompletableFuture();
    }

    @Override
    public Mono<String> publishAvro(EventEnvelope envelope, String topicName, byte[] avroBytes) {
        if (topicName == null || topicName.isEmpty()) {
            return Mono.error(new IllegalArgumentException("Topic name cannot be null or empty"));
        }
        
        Duration timeout = properties.kafka().getPublishTimeout();
        int maxRetries = properties.kafka().getMaxRetries();
        Duration retryDelay = properties.kafka().getRetryBackoffInitialDelay();
        
        return Mono.fromFuture(sendAvro(envelope, topicName, avroBytes))
                .timeout(timeout)
                .retryWhen(Retry.backoff(maxRetries, retryDelay)
                        .filter(throwable -> throwable instanceof org.apache.kafka.common.errors.TimeoutException 
                                || (throwable.getCause() != null && throwable.getCause() instanceof org.apache.kafka.common.errors.TimeoutException)
                                || throwable instanceof org.apache.kafka.common.errors.RetriableException))
                .doOnError(error -> log.error("Failed to publish Avro event {} to topic {}: {}", 
                        envelope.eventId(), topicName, error.getMessage(), error))
                .onErrorMap(throwable -> {
                    if (throwable instanceof org.apache.kafka.common.errors.TimeoutException 
                            || throwable instanceof java.util.concurrent.TimeoutException
                            || (throwable.getCause() != null && throwable.getCause() instanceof org.apache.kafka.common.errors.TimeoutException)) {
                        return new KafkaPublishException("Kafka publish timeout - broker may be unavailable for topic: " + topicName, throwable);
                    }
                    return new KafkaPublishException("Failed to publish Avro event to Kafka topic " + topicName + ": " + throwable.getMessage(), throwable);
                })
                .map(result -> {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.debug("Published Avro event {} to {}-{}@{}", envelope.eventId(), metadata.topic(), metadata.partition(), metadata.offset());
                    return envelope.eventId();
                });
    }

    private CompletableFuture<SendResult<String, byte[]>> sendAvro(EventEnvelope envelope, String topicName, byte[] avroBytes) {
        return avroKafkaTemplate.send(topicName, envelope.eventId(), avroBytes).toCompletableFuture();
    }

    private String toJson(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid event payload", e);
        }
    }
}

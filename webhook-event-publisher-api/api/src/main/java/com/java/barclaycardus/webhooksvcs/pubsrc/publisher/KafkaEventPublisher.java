package com.java.barclaycardus.webhooksvcs.pubsrc.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.barclaycardus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.java.barclaycardus.webhooksvcs.pubsrc.model.EventEnvelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final WebhooksProperties properties;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               WebhooksProperties properties,
                               ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<String> publish(EventEnvelope envelope) {
        return Mono.fromCallable(() -> toJson(envelope))
                .flatMap(payload -> Mono.fromFuture(send(envelope, payload)))
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

    private String toJson(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid event payload", e);
        }
    }
}

package com.webhooks.listener.publisher;

import com.webhooks.listener.config.ListenerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(RetryPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ListenerProperties properties;

    public RetryPublisher(KafkaTemplate<String, byte[]> kafkaTemplate, ListenerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void sendToRetry(String schemaId, byte[] payload) {
        String topic = String.format("wh.retry.%s", schemaId);
        kafkaTemplate.send(topic, schemaId, payload);
        log.warn("Sent message with schema {} to retry topic {}", schemaId, topic);
    }

    public void sendToDlq(String schemaId, byte[] payload, String reason) {
        String topic = String.format("wh.dlq.validation.%s", schemaId);
        kafkaTemplate.send(topic, schemaId, payload);
        log.error("Sent message with schema {} to DLQ {} due to {}", schemaId, topic, reason);
    }
}

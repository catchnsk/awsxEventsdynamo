package com.barclaycardus.webhooksvcs.evntsrc.publisher;

import com.barclaycardus.webhooksvcs.evntsrc.config.ListenerProperties;
import com.barclaycardus.webhooksvcs.evntsrc.model.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class MskKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(MskKafkaPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ListenerProperties properties;

    public MskKafkaPublisher(KafkaTemplate<String, byte[]> kafkaTemplate, ListenerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publish(ProcessedEvent event) {
        String topic = buildTopic(event);
        CompletableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(topic, event.partitionKey(), event.payload());
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish to topic {}", topic, ex);
                throw new IllegalStateException("Kafka publish failed", ex);
            }
            log.debug("Published event with schema {} to topic {}", event.schema().schemaId(), topic);
        });
    }

    private String buildTopic(ProcessedEvent event) {
        return event.schema().eventName();
    }
}

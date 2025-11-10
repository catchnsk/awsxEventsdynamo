package com.webhooks.listener.publisher;

import com.webhooks.listener.config.ListenerProperties;
import com.webhooks.listener.model.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

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
        kafkaTemplate.send(topic, event.partitionKey(), event.payload())
                .addCallback(new ListenableFutureCallback<>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        log.error("Failed to publish to topic {}", topic, ex);
                        throw new IllegalStateException("Kafka publish failed", ex);
                    }

                    @Override
                    public void onSuccess(Object result) {
                        log.debug("Published event with schema {} to topic {}", event.schema().schemaId(), topic);
                    }
                });
    }

    private String buildTopic(ProcessedEvent event) {
        String prefix = properties.getOutput().getMsk().getTopicPrefix();
        return String.format("%s.%s.%s", prefix, event.schema().producerDomain(), event.schema().eventName());
    }
}

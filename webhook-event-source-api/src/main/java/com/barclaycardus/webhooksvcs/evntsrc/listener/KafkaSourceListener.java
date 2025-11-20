package com.barclaycardus.webhooksvcs.evntsrc.listener;

import com.barclaycardus.webhooksvcs.evntsrc.config.KafkaConsumerConfig;
import com.barclaycardus.webhooksvcs.evntsrc.config.ListenerProperties;
import com.barclaycardus.webhooksvcs.evntsrc.model.InboundMessageContext;
import com.barclaycardus.webhooksvcs.evntsrc.service.EventProcessingService;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "listener.source.type", havingValue = "kafka")
public class KafkaSourceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceListener.class);

    private final EventProcessingService eventProcessingService;
    private final ListenerProperties properties;
    private final KafkaConsumerConfig.ConsumerFactoryManager consumerFactoryManager;
    private final Map<String, ListenerProperties.TopicConfig> topicConfigMap = new HashMap<>();

    public KafkaSourceListener(EventProcessingService eventProcessingService,
                               ListenerProperties properties,
                               KafkaConsumerConfig.ConsumerFactoryManager consumerFactoryManager) {
        this.eventProcessingService = eventProcessingService;
        this.properties = properties;
        this.consumerFactoryManager = consumerFactoryManager;
    }

    @PostConstruct
    public void init() {
        properties.getSource().getKafka().getTopics().forEach(config -> {
            topicConfigMap.put(config.getName(), config);
            registerListener(config);
        });
        log.info("Initialized Kafka listener with {} topic configurations", topicConfigMap.size());
    }

    private void registerListener(ListenerProperties.TopicConfig config) {
        ConcurrentKafkaListenerContainerFactory<String, String> containerFactory =
            consumerFactoryManager.getContainerFactory(config);

        ContainerProperties containerProperties = new ContainerProperties(config.getName());
        containerProperties.setMessageListener((MessageListener<String, String>) record -> {
            processMessage(record, config);
        });
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);

        ConcurrentMessageListenerContainer<String, String> container =
            new ConcurrentMessageListenerContainer<>(
                consumerFactoryManager.getConsumerFactory(config),
                containerProperties
            );
        container.setConcurrency(3);
        container.start();

        log.info("Registered Kafka listener for topic: {} on bootstrap servers: {}",
            config.getName(), config.getBootstrapServers());
    }

    private void processMessage(ConsumerRecord<String, String> record, ListenerProperties.TopicConfig config) {
        try {
            Map<String, Object> headers = extractHeaders(record);
            InboundMessageContext context = new InboundMessageContext(
                config.getSchemaId(),
                record.value(),
                headers,
                record.key(),
                config.getContentType()
            );
            eventProcessingService.handle(context);
        } catch (Exception e) {
            log.error("Error processing message from topic {} on bootstrap servers {}",
                config.getName(), config.getBootstrapServers(), e);
            throw e;
        }
    }

    private Map<String, Object> extractHeaders(ConsumerRecord<String, String> record) {
        Map<String, Object> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }
}

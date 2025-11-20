package com.barclaycardus.webhooksvcs.evntsrc.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    @ConditionalOnProperty(name = "listener.source.type", havingValue = "kafka")
    public ConsumerFactoryManager consumerFactoryManager(ListenerProperties properties) {
        return new ConsumerFactoryManager(properties);
    }

    @Component
    public static class ConsumerFactoryManager {

        private final Map<String, ConsumerFactory<String, String>> consumerFactories = new ConcurrentHashMap<>();
        private final Map<String, ConcurrentKafkaListenerContainerFactory<String, String>> containerFactories = new ConcurrentHashMap<>();
        private final ListenerProperties properties;

        public ConsumerFactoryManager(ListenerProperties properties) {
            this.properties = properties;
        }

        public ConsumerFactory<String, String> getConsumerFactory(ListenerProperties.TopicConfig topicConfig) {
            String key = topicConfig.getBootstrapServers() + ":" + topicConfig.getGroupId();
            return consumerFactories.computeIfAbsent(key, k -> {
                log.info("Creating consumer factory for bootstrap servers: {} with group: {}",
                    topicConfig.getBootstrapServers(), topicConfig.getGroupId());
                Map<String, Object> config = new HashMap<>();
                config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, topicConfig.getBootstrapServers());
                config.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
                config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                return new DefaultKafkaConsumerFactory<>(config);
            });
        }

        public ConcurrentKafkaListenerContainerFactory<String, String> getContainerFactory(ListenerProperties.TopicConfig topicConfig) {
            String key = topicConfig.getBootstrapServers() + ":" + topicConfig.getGroupId();
            return containerFactories.computeIfAbsent(key, k -> {
                log.info("Creating container factory for bootstrap servers: {} with group: {}",
                    topicConfig.getBootstrapServers(), topicConfig.getGroupId());
                ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
                factory.setConsumerFactory(getConsumerFactory(topicConfig));
                factory.setConcurrency(3);
                factory.getContainerProperties().setObservationEnabled(true);
                factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);
                return factory;
            });
        }
    }
}

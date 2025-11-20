package com.barclaycardus.webhooksvcs.evntsrc.config;

import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class JmsConfig {

    private static final Logger log = LoggerFactory.getLogger(JmsConfig.class);

    @Bean
    @ConditionalOnProperty(name = "listener.source.type", havingValue = "amq", matchIfMissing = true)
    public ConnectionFactoryManager connectionFactoryManager(ListenerProperties properties) {
        return new ConnectionFactoryManager(properties);
    }

    @Component
    public static class ConnectionFactoryManager {

        private final Map<String, ConnectionFactory> connectionFactories = new ConcurrentHashMap<>();
        private final Map<String, DefaultJmsListenerContainerFactory> containerFactories = new ConcurrentHashMap<>();
        private final ListenerProperties properties;

        public ConnectionFactoryManager(ListenerProperties properties) {
            this.properties = properties;
        }

        public ConnectionFactory getConnectionFactory(ListenerProperties.QueueConfig queueConfig) {
            return connectionFactories.computeIfAbsent(queueConfig.getBrokerUrl(), brokerUrl -> {
                log.info("Creating connection factory for broker: {}", brokerUrl);
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
                if (queueConfig.getUsername() != null) {
                    factory.setUserName(queueConfig.getUsername());
                    factory.setPassword(queueConfig.getPassword());
                }
                factory.setTrustAllPackages(true);
                return new CachingConnectionFactory(factory);
            });
        }

        public DefaultJmsListenerContainerFactory getContainerFactory(ListenerProperties.QueueConfig queueConfig) {
            return containerFactories.computeIfAbsent(queueConfig.getBrokerUrl(), brokerUrl -> {
                log.info("Creating container factory for broker: {}", brokerUrl);
                DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
                factory.setConnectionFactory(getConnectionFactory(queueConfig));
                factory.setConcurrency("3-10");
                factory.setSessionTransacted(true);
                factory.setErrorHandler(t -> log.error("JMS listener error for broker {}", brokerUrl, t));
                return factory;
            });
        }
    }
}

package com.webhooks.listener.config;

import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;

@Configuration
public class JmsConfig {

    private static final Logger log = LoggerFactory.getLogger(JmsConfig.class);

    @Bean
    @ConditionalOnProperty(name = "listener.source.type", havingValue = "amq", matchIfMissing = true)
    public ConnectionFactory amqConnectionFactory(ListenerProperties properties) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(properties.getSource().getAmq().getBrokerUrl());
        if (properties.getSource().getAmq().getUsername() != null) {
            factory.setUserName(properties.getSource().getAmq().getUsername());
            factory.setPassword(properties.getSource().getAmq().getPassword());
        }
        factory.setTrustAllPackages(true);
        return new CachingConnectionFactory(factory);
    }

    @Bean
    @ConditionalOnProperty(name = "listener.source.type", havingValue = "amq", matchIfMissing = true)
    public JmsListenerContainerFactory<?> amqListenerContainerFactory(ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setConcurrency("3-10");
        factory.setSessionTransacted(true);
        factory.setErrorHandler(t -> log.error("JMS listener error", t));
        return factory;
    }
}

package com.barclaycardus.webhooksvcs.evntsrc.listener;

import com.barclaycardus.webhooksvcs.evntsrc.config.JmsConfig;
import com.barclaycardus.webhooksvcs.evntsrc.config.ListenerProperties;
import com.barclaycardus.webhooksvcs.evntsrc.model.InboundMessageContext;
import com.barclaycardus.webhooksvcs.evntsrc.service.EventProcessingService;
import jakarta.annotation.PostConstruct;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "listener.source.type", havingValue = "amq", matchIfMissing = true)
public class AmqListener {

    private static final Logger log = LoggerFactory.getLogger(AmqListener.class);

    private final EventProcessingService eventProcessingService;
    private final ListenerProperties properties;
    private final JmsListenerEndpointRegistry registry;
    private final JmsConfig.ConnectionFactoryManager connectionFactoryManager;
    private final Map<String, ListenerProperties.QueueConfig> queueConfigMap = new HashMap<>();

    public AmqListener(EventProcessingService eventProcessingService,
                       ListenerProperties properties,
                       JmsListenerEndpointRegistry registry,
                       JmsConfig.ConnectionFactoryManager connectionFactoryManager) {
        this.eventProcessingService = eventProcessingService;
        this.properties = properties;
        this.registry = registry;
        this.connectionFactoryManager = connectionFactoryManager;
    }

    @PostConstruct
    public void init() {
        properties.getSource().getAmq().getQueues().forEach(config -> {
            queueConfigMap.put(config.getName(), config);
            registerListener(config);
        });
        log.info("Initialized AMQ listener with {} queue configurations", queueConfigMap.size());
    }

    private void registerListener(ListenerProperties.QueueConfig config) {
        SimpleJmsListenerEndpoint endpoint = new SimpleJmsListenerEndpoint();
        endpoint.setId("amq-listener-" + config.getName());
        endpoint.setDestination(config.getName());
        endpoint.setMessageListener(message -> {
            try {
                processMessage(message, config);
            } catch (JMSException e) {
                log.error("Error processing message from queue {} on broker {}",
                    config.getName(), config.getBrokerUrl(), e);
                throw new RuntimeException("Message processing failed", e);
            }
        });

        // Get or create container factory for this queue's broker
        DefaultJmsListenerContainerFactory containerFactory = connectionFactoryManager.getContainerFactory(config);

        // Register endpoint with the registry using the container factory
        registry.registerListenerContainer(endpoint, containerFactory, true);

        log.info("Registered JMS listener for queue: {} on broker: {}", config.getName(), config.getBrokerUrl());
    }

    private void processMessage(Message message, ListenerProperties.QueueConfig config) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            Map<String, Object> headers = extractHeaders(message);
            InboundMessageContext context = new InboundMessageContext(
                config.getSchemaId(),
                textMessage.getText(),
                headers,
                message.getJMSCorrelationID(),
                config.getContentType()
            );
            eventProcessingService.handle(context);
        } else {
            log.warn("Unsupported JMS message type {}", message.getClass());
        }
    }

    private Map<String, Object> extractHeaders(Message message) throws JMSException {
        Map<String, Object> headers = new HashMap<>();
        Enumeration<String> names = message.getPropertyNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            headers.put(name, message.getObjectProperty(name));
        }
        return headers;
    }
}

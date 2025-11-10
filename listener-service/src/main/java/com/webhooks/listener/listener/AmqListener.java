package com.webhooks.listener.listener;

import com.webhooks.listener.model.InboundMessageContext;
import com.webhooks.listener.service.EventProcessingService;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "listener.source.type", havingValue = "amq", matchIfMissing = true)
public class AmqListener {

    private static final Logger log = LoggerFactory.getLogger(AmqListener.class);

    private final EventProcessingService eventProcessingService;

    public AmqListener(EventProcessingService eventProcessingService) {
        this.eventProcessingService = eventProcessingService;
    }

    @JmsListener(destination = "${listener.source.amq.queue}", containerFactory = "amqListenerContainerFactory")
    public void onMessage(Message message) throws JMSException {
        if (message instanceof TextMessage textMessage) {
            String schemaId = message.getStringProperty("schemaId");
            Map<String, Object> headers = extractHeaders(message);
            InboundMessageContext context = new InboundMessageContext(schemaId, textMessage.getText(), headers, message.getJMSCorrelationID());
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

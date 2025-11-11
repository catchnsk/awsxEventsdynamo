package com.barclaycardus.webhooksvcs.evntsrc.listener;

import com.barclaycardus.webhooksvcs.evntsrc.model.InboundMessageContext;
import com.barclaycardus.webhooksvcs.evntsrc.service.EventProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "listener.source.type", havingValue = "kafka")
public class KafkaSourceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceListener.class);

    private final EventProcessingService eventProcessingService;

    public KafkaSourceListener(EventProcessingService eventProcessingService) {
        this.eventProcessingService = eventProcessingService;
    }

    @KafkaListener(topics = "#{@kafkaSourceTopics}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        Map<String, Object> headers = extractHeaders(record);
        String schemaId = findHeader(record, "schemaId");
        InboundMessageContext context = new InboundMessageContext(schemaId, record.value(), headers, record.key());
        eventProcessingService.handle(context);
        acknowledgment.acknowledge();
    }

    private Map<String, Object> extractHeaders(ConsumerRecord<String, String> record) {
        Map<String, Object> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headers;
    }

    private String findHeader(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}

package com.webhooks.reference.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.reference.config.WebhookProducerProperties;
import com.webhooks.reference.model.CustomerUpdateRequest;
import com.webhooks.sdk.core.Event;
import com.webhooks.sdk.core.PublishResponse;
import com.webhooks.sdk.core.SchemaReference;
import com.webhooks.sdk.core.WebhookClient;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class DemoEventService {

    private final WebhookClient webhookClient;
    private final ObjectMapper objectMapper;
    private final SchemaReference schemaReference;

    public DemoEventService(WebhookClient webhookClient,
                            ObjectMapper objectMapper,
                            WebhookProducerProperties properties) {
        this.webhookClient = webhookClient;
        this.objectMapper = objectMapper;
        this.schemaReference = SchemaReference.of(properties.domain(), properties.eventName(), properties.schemaVersion());
    }

    public PublishResponse publishCustomerUpdate(String customerId, CustomerUpdateRequest request) {
        try {
            var payload = objectMapper.createObjectNode()
                    .put("customerId", customerId)
                    .put("status", request.status())
                    .put("email", request.email());

            Event event = Event.builder(schemaReference)
                    .payload(payload)
                    .timestamp(Instant.now())
                    .headers(request.headers())
                    .build();

            return webhookClient.publish(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish event", e);
        }
    }
}

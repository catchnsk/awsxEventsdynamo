package com.webhooks.sdk.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class WebhookClientTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void publishesEventWhenSchemaValid() throws Exception {
        SchemaReference reference = SchemaReference.of("crm", "CustomerUpdated", "v1");
        WebhookTransport transport = event -> PublishResponse.accepted(event.id(), Instant.now(), Duration.ZERO);

        WebhookClient client = WebhookClient.builder()
                .transport(transport)
                .schemaFetcher(ref -> new SchemaDefinition(ref, """
                        {
                          "$schema": "http://json-schema.org/draft-07/schema#",
                          "type": "object",
                          "properties": { "customerId": { "type": "string" } },
                          "required": ["customerId"]
                        }
                        """, Instant.now()))
                .build();

        Event event = Event.builder(reference)
                .payload(objectMapper.readTree("{\"customerId\":\"123\"}"))
                .build();

        PublishResponse response = client.publish(event);

        assertThat(response.accepted()).isTrue();
    }
}

package com.webhooks.sdk.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.sdk.core.Event;
import com.webhooks.sdk.core.PublishResponse;
import com.webhooks.sdk.core.SchemaReference;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaWebhookPublisherTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void writesRecordToKafka() throws Exception {
        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaWebhookPublisher publisher = KafkaWebhookPublisher.builder()
                .topic("wh.ingress.crm.CustomerUpdated")
                .producerSupplier(() -> mockProducer)
                .build();

        Event event = Event.builder(SchemaReference.of("crm", "CustomerUpdated", "v1"))
                .payload(objectMapper.readTree("{\"customerId\":\"123\"}"))
                .build();

        PublishResponse response = publisher.send(event);

        assertThat(response.accepted()).isTrue();
        assertThat(mockProducer.history()).hasSize(1);
    }
}

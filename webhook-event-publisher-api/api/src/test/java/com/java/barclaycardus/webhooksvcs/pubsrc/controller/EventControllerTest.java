package com.java.barclaycardus.webhooksvcs.pubsrc.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.barclaycardus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.java.barclaycardus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaService;
import com.java.barclaycardus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.Instant;

@WebFluxTest(controllers = EventController.class)
@Import({EventControllerTest.TestConfig.class, JsonSchemaValidator.class})
class EventControllerTest {

    private final WebTestClient webTestClient;

    EventControllerTest(WebTestClient webTestClient) {
        this.webTestClient = webTestClient;
    }

    @Test
    void acceptsValidEvent() {
        webTestClient.post()
                .uri("/events/CustomerUpdated")
                .header("X-Producer-Domain", "demo")
                .header("X-Event-Version", "v1")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists();
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        SchemaService schemaService() {
            SchemaDefinition definition = new SchemaDefinition(
                    new SchemaReference("demo", "CustomerUpdated", "v1"),
                    """
                            {
                              "$schema":"http://json-schema.org/draft-07/schema#",
                              "type":"object",
                              "properties":{"customerId":{"type":"string"}},
                              "required":["customerId"]
                            }
                            """,
                    true,
                    Instant.now()
            );
            return reference -> Mono.just(definition);
        }

        @Bean
        EventPublisher eventPublisher() {
            return envelope -> Mono.just(envelope.eventId());
        }
    }
}

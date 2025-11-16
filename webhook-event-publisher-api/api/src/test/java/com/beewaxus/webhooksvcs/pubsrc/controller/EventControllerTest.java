package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.beewaxus.webhooksvcs.pubsrc.converter.AvroSerializer;
import com.beewaxus.webhooksvcs.pubsrc.converter.FormatConverter;
import com.beewaxus.webhooksvcs.pubsrc.validation.AvroSchemaValidator;
import com.beewaxus.webhooksvcs.pubsrc.validation.XmlSchemaValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.beewaxus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.beewaxus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDetailResponse;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;
import com.beewaxus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@WebFluxTest(controllers = EventController.class)
@Import({EventControllerTest.TestConfig.class})
class EventControllerTest {

    private final WebTestClient webTestClient;

    @Autowired
    EventControllerTest(WebTestClient webTestClient) {
        this.webTestClient = webTestClient;
    }

    @Test
    void acceptsValidEvent() {
        webTestClient.post()
                .uri("/events/CustomerUpdated")
                .header("X-Producer-Domain", "demo")
                .header("X-Event-Version", "v1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists();
    }

    @Test
    void publishEventBySchemaId_WithValidJsonPayload_ReturnsAccepted() {
        webTestClient.post()
                .uri("/events/schema_id/SCHEMA_0001")
                .header("Content-Type", "application/json")
                .header("X-Event-Id", "evt-test-123")
                .header("Idempotency-Key", "idem-test-456")
                .bodyValue("{\"customerId\":\"123\",\"status\":\"ACTIVE\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists()
                .jsonPath("$.eventId").isEqualTo("evt-test-123");
    }

    @Test
    void publishEventBySchemaId_WithValidXmlPayload_ReturnsAccepted() {
        webTestClient.post()
                .uri("/events/schema_id/SCHEMA_0001")
                .header("Content-Type", "application/xml")
                .header("X-Event-Id", "evt-test-456")
                .bodyValue("<event><customerId>123</customerId><status>ACTIVE</status></event>")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists();
    }

    @Test
    void publishEventBySchemaId_WithValidAvroPayload_ReturnsAccepted() {
        webTestClient.post()
                .uri("/events/schema_id/SCHEMA_0001")
                .header("Content-Type", "application/avro")
                .header("X-Event-Id", "evt-test-789")
                .bodyValue("{\"customerId\":\"123\",\"status\":\"ACTIVE\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists();
    }

    @Test
    void publishEventBySchemaId_WhenSchemaNotFound_ReturnsNotFound() {
        webTestClient.post()
                .uri("/events/schema_id/NONEXISTENT_SCHEMA")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.error").exists();
    }

    @Test
    void publishEventBySchemaId_WhenSchemaInactive_ReturnsBadRequest() {
        webTestClient.post()
                .uri("/events/schema_id/INACTIVE_SCHEMA")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void publishEventBySchemaId_WhenTopicInactive_ReturnsBadRequest() {
        webTestClient.post()
                .uri("/events/schema_id/INACTIVE_TOPIC_SCHEMA")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void publishEventBySchemaId_WhenTopicNameMissing_ReturnsBadRequest() {
        webTestClient.post()
                .uri("/events/schema_id/NO_TOPIC_SCHEMA")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void publishEventBySchemaId_WhenAvroSchemaMissing_ReturnsBadRequest() {
        webTestClient.post()
                .uri("/events/schema_id/NO_AVRO_SCHEMA")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void publishEventBySchemaId_WhenBodyMissing_ReturnsBadRequest() {
        webTestClient.post()
                .uri("/events/schema_id/SCHEMA_0001")
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void publishEventBySchemaId_GeneratesEventId_WhenNotProvided() {
        webTestClient.post()
                .uri("/events/schema_id/SCHEMA_0001")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\",\"status\":\"ACTIVE\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists()
                .jsonPath("$.eventId").isNotEmpty();
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
                    null, // xmlSchema
                    null, // avroSchema
                    true,
                    Instant.now()
            );
            return new SchemaService() {
                @Override
                public Mono<SchemaDefinition> fetchSchema(SchemaReference reference) {
                    return Mono.just(definition);
                }

                @Override
                public Flux<SchemaDefinition> fetchAllSchemas() {
                    return Flux.just(definition);
                }

                @Override
                public Mono<SchemaDetailResponse> fetchSchemaBySchemaId(String schemaId) {
                    if ("NONEXISTENT_SCHEMA".equals(schemaId)) {
                        return Mono.empty();
                    }
                    
                    String simpleXmlSchema = """
                            <?xml version="1.0" encoding="UTF-8"?>
                            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                                <xs:element name="event">
                                    <xs:complexType>
                                        <xs:sequence>
                                            <xs:element name="customerId" type="xs:string" minOccurs="0"/>
                                            <xs:element name="status" type="xs:string" minOccurs="0"/>
                                        </xs:sequence>
                                    </xs:complexType>
                                </xs:element>
                            </xs:schema>
                            """;
                    String simpleAvroSchema = "{\"type\":\"record\",\"name\":\"TestEvent\",\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"}]}";
                    
                    if ("INACTIVE_SCHEMA".equals(schemaId)) {
                        return Mono.just(new SchemaDetailResponse(
                                "INACTIVE_SCHEMA",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                "{\"type\":\"object\",\"properties\":{\"customerId\":{\"type\":\"string\"}}}",
                                simpleXmlSchema,
                                simpleAvroSchema,
                                null,
                                "INACTIVE",
                                "NO",
                                "user1",
                                "test.topic",
                                "ACTIVE",
                                Instant.now(),
                                "user1",
                                Instant.now(),
                                "user1"
                        ));
                    }
                    
                    if ("INACTIVE_TOPIC_SCHEMA".equals(schemaId)) {
                        return Mono.just(new SchemaDetailResponse(
                                "INACTIVE_TOPIC_SCHEMA",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                "{\"type\":\"object\",\"properties\":{\"customerId\":{\"type\":\"string\"}}}",
                                simpleXmlSchema,
                                simpleAvroSchema,
                                null,
                                "ACTIVE",
                                "NO",
                                "user1",
                                "test.topic",
                                "INACTIVE",
                                Instant.now(),
                                "user1",
                                Instant.now(),
                                "user1"
                        ));
                    }
                    
                    if ("NO_TOPIC_SCHEMA".equals(schemaId)) {
                        return Mono.just(new SchemaDetailResponse(
                                "NO_TOPIC_SCHEMA",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                "{\"type\":\"object\",\"properties\":{\"customerId\":{\"type\":\"string\"}}}",
                                simpleXmlSchema,
                                simpleAvroSchema,
                                null,
                                "ACTIVE",
                                "NO",
                                "user1",
                                null,
                                "ACTIVE",
                                Instant.now(),
                                "user1",
                                Instant.now(),
                                "user1"
                        ));
                    }
                    
                    if ("NO_AVRO_SCHEMA".equals(schemaId)) {
                        return Mono.just(new SchemaDetailResponse(
                                "NO_AVRO_SCHEMA",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                "{\"type\":\"object\",\"properties\":{\"customerId\":{\"type\":\"string\"}}}",
                                simpleXmlSchema,
                                null,
                                null,
                                "ACTIVE",
                                "NO",
                                "user1",
                                "test.topic",
                                "ACTIVE",
                                Instant.now(),
                                "user1",
                                Instant.now(),
                                "user1"
                        ));
                    }
                    
                    // Default: Return valid schema for SCHEMA_0001
                    // XSD schema matching the actual XML structure with eventHeader and eventPayload
                    String xmlSchema = """
                            <?xml version="1.0" encoding="UTF-8"?>
                            <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                                <xs:element name="event">
                                    <xs:complexType>
                                        <xs:sequence>
                                            <xs:element name="eventHeader" minOccurs="0">
                                                <xs:complexType>
                                                    <xs:sequence>
                                                        <xs:element name="eventId" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="eventName" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="producerDomain" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="version" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="timestamp" type="xs:string" minOccurs="0"/>
                                                    </xs:sequence>
                                                </xs:complexType>
                                            </xs:element>
                                            <xs:element name="eventPayload" minOccurs="0">
                                                <xs:complexType>
                                                    <xs:sequence>
                                                        <xs:element name="transactionId" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="customerId" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="amount" type="xs:decimal" minOccurs="0"/>
                                                        <xs:element name="currency" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="status" type="xs:string" minOccurs="0"/>
                                                        <xs:element name="metadata" minOccurs="0">
                                                            <xs:complexType>
                                                                <xs:sequence>
                                                                    <xs:any processContents="skip" minOccurs="0" maxOccurs="unbounded"/>
                                                                </xs:sequence>
                                                            </xs:complexType>
                                                        </xs:element>
                                                    </xs:sequence>
                                                </xs:complexType>
                                            </xs:element>
                                            <xs:element name="customerId" type="xs:string" minOccurs="0"/>
                                            <xs:element name="status" type="xs:string" minOccurs="0"/>
                                        </xs:sequence>
                                    </xs:complexType>
                                </xs:element>
                            </xs:schema>
                            """;
                    
                    return Mono.just(new SchemaDetailResponse(
                            "SCHEMA_0001",
                            "demo",
                            "TestEvent",
                            "1.0",
                            "Test header",
                            "{\"type\":\"object\",\"properties\":{\"customerId\":{\"type\":\"string\"},\"status\":{\"type\":\"string\"}},\"required\":[\"customerId\"]}",
                            xmlSchema,
                            "{\"type\":\"record\",\"name\":\"TestEvent\",\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"}]}",
                            null,
                            "ACTIVE",
                            "NO",
                            "user1",
                            "test.topic",
                            "ACTIVE",
                            Instant.now(),
                            "user1",
                            Instant.now(),
                            "user1"
                    ));
                }
            };
        }

        @Bean
        EventPublisher eventPublisher() {
            return new EventPublisher() {
                @Override
                public Mono<String> publish(EventEnvelope envelope) {
                    return Mono.just(envelope.eventId());
                }

                @Override
                public Mono<String> publishAvro(EventEnvelope envelope, String topicName, byte[] avroBytes) {
                    return Mono.just(envelope.eventId());
                }
            };
        }

        @Bean
        AvroSerializer avroSerializer() {
            return new AvroSerializer();
        }

        @Bean
        JsonSchemaValidator jsonSchemaValidator() {
            return new JsonSchemaValidator();
        }

        @Bean
        XmlSchemaValidator xmlSchemaValidator() {
            return new XmlSchemaValidator();
        }

        @Bean
        AvroSchemaValidator avroSchemaValidator() {
            return new AvroSchemaValidator();
        }

        @Bean
        FormatConverter formatConverter() {
            return new FormatConverter();
        }
    }
}

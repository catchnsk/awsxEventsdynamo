package com.beewaxus.webhooksvcs.pubsrc.controller;

import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.beewaxus.webhooksvcs.pubsrc.converter.AvroSerializer;
import com.beewaxus.webhooksvcs.pubsrc.converter.FormatConverter;
import com.beewaxus.webhooksvcs.pubsrc.validation.AvroSchemaValidator;
import com.beewaxus.webhooksvcs.pubsrc.validation.JsonSchemaValidator;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormatType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.beewaxus.webhooksvcs.pubsrc.model.EventEnvelope;
import com.beewaxus.webhooksvcs.pubsrc.publisher.EventPublisher;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDetailResponse;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaService;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@WebFluxTest(controllers = {EventController.class, ApiExceptionHandler.class})
@Import({EventControllerTest.TestConfig.class})
class EventControllerTest {

    @MockBean
    private DynamoDbClient dynamoDbClient;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    private KafkaTemplate<String, byte[]> avroKafkaTemplate;

    private final WebTestClient webTestClient;

    @Autowired
    EventControllerTest(WebTestClient webTestClient) {
        this.webTestClient = webTestClient;
    }

    @Test
    void acceptsValidEvent() {
        webTestClient.post()
                .uri("/events/demo/CustomerUpdated/v1")
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
    void publishEventBySchemaId_WhenSchemaDefinitionMissing_ReturnsBadRequest() {
        webTestClient.post()
                .uri("/events/schema_id/NO_SCHEMA_DEFINITION")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void publishEventBySchemaId_WithFallbackToAvroSchema_ReturnsAccepted() {
        // Test that when EVENT_SCHEMA_DEFINITION is null, it falls back to EVENT_SCHEMA_DEFINITION_AVRO
        webTestClient.post()
                .uri("/events/schema_id/FALLBACK_SCHEMA")
                .header("Content-Type", "application/json")
                .header("X-Event-Id", "evt-fallback-test")
                .bodyValue("{\"customerId\":\"123\",\"status\":\"ACTIVE\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists()
                .jsonPath("$.eventId").isEqualTo("evt-fallback-test");
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

    @Test
    void publishEvent_WithJsonSchema_ReturnsAccepted() {
        webTestClient.post()
                .uri("/events/demo/UserEvent/v1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"userId\":\"user123\",\"action\":\"LOGIN\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists();
    }

    @Test
    void publishEvent_WithAvroSchema_ReturnsAccepted() {
        webTestClient.post()
                .uri("/events/demo/CustomerUpdated/v1")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerId\":\"123\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").exists();
    }

    @Test
    void publishEventBySchemaId_WithJsonSchema_ReturnsAccepted() {
        webTestClient.post()
                .uri("/events/schema_id/JSON_SCHEMA_001")
                .header("Content-Type", "application/json")
                .header("X-Event-Id", "evt-json-test")
                .bodyValue("{\"userId\":\"user123\",\"action\":\"LOGIN\"}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.eventId").isEqualTo("evt-json-test");
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Primary
        SchemaService schemaService() {
            String avroSchema = "{\"type\":\"record\",\"name\":\"CustomerUpdated\",\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"}]}";
            SchemaDefinition avroDefinition = new SchemaDefinition(
                    new SchemaReference("demo", "CustomerUpdated", "v1"),
                    null,  // jsonSchema
                    avroSchema,  // avroSchema
                    SchemaFormatType.AVRO_SCHEMA,  // formatType
                    true,
                    Instant.now(),
                    null  // eventSchemaId
            );

            String jsonSchema = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "userId": { "type": "string" },
                    "action": { "type": "string" }
                  },
                  "required": ["userId", "action"]
                }
                """;
            SchemaDefinition jsonDefinition = new SchemaDefinition(
                    new SchemaReference("demo", "UserEvent", "v1"),
                    jsonSchema,  // jsonSchema
                    null,  // avroSchema
                    SchemaFormatType.JSON_SCHEMA,  // formatType
                    true,
                    Instant.now(),
                    null  // eventSchemaId
            );

            return new SchemaService() {
                @Override
                public Mono<SchemaDefinition> fetchSchema(SchemaReference reference) {
                    if ("UserEvent".equals(reference.eventName())) {
                        return Mono.just(jsonDefinition);
                    }
                    return Mono.just(avroDefinition);
                }

                @Override
                public Flux<SchemaDefinition> fetchAllSchemas() {
                    return Flux.just(avroDefinition, jsonDefinition);
                }

                @Override
                public Mono<SchemaDetailResponse> fetchSchemaBySchemaId(String schemaId) {
                    if ("NONEXISTENT_SCHEMA".equals(schemaId)) {
                        return Mono.empty();
                    }
                    
                    String simpleAvroSchema = "{\"type\":\"record\",\"name\":\"TestEvent\",\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"}]}";
                    
                    if ("INACTIVE_SCHEMA".equals(schemaId)) {
                        return Mono.just(new SchemaDetailResponse(
                                "INACTIVE_SCHEMA",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                simpleAvroSchema, // eventSchemaDefinitionAvro
                                null, // eventSchemaDefinition (null = use Avro)
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
                                simpleAvroSchema, // eventSchemaDefinitionAvro
                                null, // eventSchemaDefinition (null = use Avro)
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
                                simpleAvroSchema, // eventSchemaDefinitionAvro
                                null, // eventSchemaDefinition (null = use Avro)
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
                    
                    if ("NO_SCHEMA_DEFINITION".equals(schemaId)) {
                        return Mono.just(new SchemaDetailResponse(
                                "NO_SCHEMA_DEFINITION",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                null, // eventSchemaDefinitionAvro (fallback)
                                null, // eventSchemaDefinition (primary - missing, should fail)
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
                    
                    if ("FALLBACK_SCHEMA".equals(schemaId)) {
                        // Test fallback: EVENT_SCHEMA_DEFINITION is null, but EVENT_SCHEMA_DEFINITION_AVRO exists
                        String fallbackAvroSchema = "{\"type\":\"record\",\"name\":\"TestEvent\",\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"}]}";
                        return Mono.just(new SchemaDetailResponse(
                                "FALLBACK_SCHEMA",
                                "demo",
                                "TestEvent",
                                "1.0",
                                "Test header",
                                fallbackAvroSchema, // eventSchemaDefinitionAvro (fallback - will be used)
                                null, // eventSchemaDefinition (primary - null, so falls back)
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
                    
                    if ("JSON_SCHEMA_001".equals(schemaId)) {
                        // Test JSON Schema flow
                        String jsonSchemaStr = """
                            {
                              "$schema": "http://json-schema.org/draft-07/schema#",
                              "type": "object",
                              "properties": {
                                "userId": { "type": "string" },
                                "action": { "type": "string" }
                              },
                              "required": ["userId", "action"]
                            }
                            """;
                        return Mono.just(new SchemaDetailResponse(
                                "JSON_SCHEMA_001",
                                "demo",
                                "UserEvent",
                                "1.0",
                                "Test header",
                                null, // eventSchemaDefinitionAvro (not used)
                                jsonSchemaStr, // eventSchemaDefinition (JSON Schema)
                                null,
                                "ACTIVE",
                                "NO",
                                "user1",
                                "test.topic.user",
                                "ACTIVE",
                                Instant.now(),
                                "user1",
                                Instant.now(),
                                "user1"
                        ));
                    }

                    // Default: Return valid schema for SCHEMA_0001 (Avro schema)
                    String avroSchema = "{\"type\":\"record\",\"name\":\"TestEvent\",\"fields\":[{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"}]}";

                    return Mono.just(new SchemaDetailResponse(
                            "SCHEMA_0001",
                            "demo",
                            "TestEvent",
                            "1.0",
                            "Test header",
                            avroSchema, // eventSchemaDefinitionAvro
                            null, // eventSchemaDefinition (null = use Avro schema)
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

                @Override
                public Mono<Void> evictAndReload() {
                    return Mono.empty();
                }
            };
        }

        @Bean
        @Primary
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

                @Override
                public Mono<String> publishJson(EventEnvelope envelope, String topicName, JsonNode jsonPayload) {
                    return Mono.just(envelope.eventId());
                }
            };
        }

        @Bean
        @Primary
        AvroSerializer avroSerializer() {
            return new AvroSerializer();
        }

        @Bean
        @Primary
        AvroSchemaValidator avroSchemaValidator() {
            return new AvroSchemaValidator();
        }

        @Bean
        @Primary
        JsonSchemaValidator jsonSchemaValidator() {
            return new JsonSchemaValidator();
        }

        @Bean
        @Primary
        FormatConverter formatConverter() {
            return new FormatConverter();
        }

        @Bean
        @Primary
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        @Primary
        WebhooksProperties webhooksProperties() {
            // Create test WebhooksProperties with default values
            return new WebhooksProperties(
                    new WebhooksProperties.DynamoProperties(
                            "event_schema",
                            "EVENT_IDEMPOTENCY_LEDGER"
                    ),
                    new WebhooksProperties.KafkaProperties(
                            "localhost:9092",
                            "wh.ingress",
                            Duration.ofSeconds(30),
                            2,
                            Duration.ofSeconds(1)
                    ),
                    new WebhooksProperties.CacheProperties(
                            true,
                            Duration.ofMinutes(5),
                            Duration.ofMinutes(5),
                            100
                    )
            );
        }
    }
}

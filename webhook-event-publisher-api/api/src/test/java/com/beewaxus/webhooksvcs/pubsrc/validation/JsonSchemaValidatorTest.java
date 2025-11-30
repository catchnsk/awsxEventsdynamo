package com.beewaxus.webhooksvcs.pubsrc.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaFormatType;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;

class JsonSchemaValidatorTest {

    private JsonSchemaValidator validator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        validator = new JsonSchemaValidator();
        objectMapper = new ObjectMapper();
    }

    @Test
    void validate_WithValidPayload_ReturnsPayload() throws Exception {
        String jsonSchemaString = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "customerId": { "type": "string" },
                    "status": { "type": "string" }
                  },
                  "required": ["customerId"]
                }
                """;

        SchemaDefinition schemaDefinition = new SchemaDefinition(
                new SchemaReference("demo", "CustomerUpdated", "v1"),
                jsonSchemaString,
                null,
                SchemaFormatType.JSON_SCHEMA,
                true,
                Instant.now()
        );

        String payload = "{\"customerId\":\"123\",\"status\":\"ACTIVE\"}";
        JsonNode jsonNode = objectMapper.readTree(payload);

        StepVerifier.create(validator.validate(jsonNode, schemaDefinition))
                .expectNext(jsonNode)
                .verifyComplete();
    }

    @Test
    void validate_WithInvalidPayload_ThrowsValidationException() throws Exception {
        String jsonSchemaString = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "customerId": { "type": "string" },
                    "status": { "type": "string" }
                  },
                  "required": ["customerId"]
                }
                """;

        SchemaDefinition schemaDefinition = new SchemaDefinition(
                new SchemaReference("demo", "CustomerUpdated", "v1"),
                jsonSchemaString,
                null,
                SchemaFormatType.JSON_SCHEMA,
                true,
                Instant.now()
        );

        // Missing required field customerId
        String payload = "{\"status\":\"ACTIVE\"}";
        JsonNode jsonNode = objectMapper.readTree(payload);

        StepVerifier.create(validator.validate(jsonNode, schemaDefinition))
                .expectError(SchemaValidationException.class)
                .verify();
    }

    @Test
    void validate_WithMismatchedType_ThrowsValidationException() throws Exception {
        String jsonSchemaString = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "customerId": { "type": "string" },
                    "age": { "type": "number" }
                  },
                  "required": ["customerId", "age"]
                }
                """;

        SchemaDefinition schemaDefinition = new SchemaDefinition(
                new SchemaReference("demo", "CustomerUpdated", "v1"),
                jsonSchemaString,
                null,
                SchemaFormatType.JSON_SCHEMA,
                true,
                Instant.now()
        );

        // age should be number, not string
        String payload = "{\"customerId\":\"123\",\"age\":\"thirty\"}";
        JsonNode jsonNode = objectMapper.readTree(payload);

        StepVerifier.create(validator.validate(jsonNode, schemaDefinition))
                .expectError(SchemaValidationException.class)
                .verify();
    }

    @Test
    void validate_WhenSchemaIsNull_ThrowsException() throws Exception {
        SchemaDefinition schemaDefinition = new SchemaDefinition(
                new SchemaReference("demo", "CustomerUpdated", "v1"),
                null,
                null,
                SchemaFormatType.JSON_SCHEMA,
                true,
                Instant.now()
        );

        String payload = "{\"customerId\":\"123\"}";
        JsonNode jsonNode = objectMapper.readTree(payload);

        StepVerifier.create(validator.validate(jsonNode, schemaDefinition))
                .expectError(SchemaValidationException.class)
                .verify();
    }
}

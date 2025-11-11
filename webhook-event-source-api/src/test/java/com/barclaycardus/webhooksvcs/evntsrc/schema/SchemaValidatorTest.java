package com.barclaycardus.webhooksvcs.evntsrc.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.barclaycardus.webhooksvcs.evntsrc.error.SchemaValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaValidatorTest {

    private final SchemaValidator validator = new SchemaValidator(new ObjectMapper());

    @Test
    void validatesJsonSchemaSuccessfully() {
        SchemaMetadata schema = new SchemaMetadata(
                "customer-created",
                """
                {
                  "$schema": "https://json-schema.org/draft/2019-09/schema",
                  "type": "object",
                  "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"}
                  },
                  "required": ["id", "name"]
                }
                """,
                SchemaMetadata.FormatType.JSON,
                false,
                SchemaMetadata.Status.ACTIVE,
                "customer",
                "created"
        );

        assertDoesNotThrow(() -> validator.validate(schema, "{\"id\":\"123\",\"name\":\"Ada\"}"));
    }

    @Test
    void throwsOnInvalidJson() {
        SchemaMetadata schema = new SchemaMetadata(
                "customer-created",
                """
                {
                  "$schema": "https://json-schema.org/draft/2019-09/schema",
                  "type": "object",
                  "properties": {
                    "id": {"type": "string"}
                  },
                  "required": ["id"]
                }
                """,
                SchemaMetadata.FormatType.JSON,
                false,
                SchemaMetadata.Status.ACTIVE,
                "customer",
                "created"
        );

        assertThrows(SchemaValidationException.class, () -> validator.validate(schema, "{}"));
    }
}

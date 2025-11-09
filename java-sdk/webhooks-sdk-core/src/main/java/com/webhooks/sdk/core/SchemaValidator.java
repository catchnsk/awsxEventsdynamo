package com.webhooks.sdk.core;

import com.fasterxml.jackson.databind.JsonNode;
import org.everit.json.schema.JsonSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

/**
 * Validates payloads against JSON Schema documents.
 */
public final class SchemaValidator {

    public void validate(JsonNode payload, SchemaDefinition definition) {
        JSONObject rawSchema = new JSONObject(definition.rawSchema());
        JsonSchema jsonSchema = SchemaLoader.load(rawSchema);
        jsonSchema.validate(new JSONObject(payload.toString()));
    }
}

package com.beewaxus.webhooksvcs.pubsrc.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Set;
import java.util.stream.Collectors;

@Component
public class JsonSchemaValidator {

    private final JsonSchemaFactory schemaFactory;

    public JsonSchemaValidator() {
        this.schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
    }

    public Mono<JsonNode> validate(JsonNode payload, SchemaDefinition schemaDefinition) {
        return Mono.fromCallable(() -> {
                    String jsonSchemaString = schemaDefinition.jsonSchema();

                    if (jsonSchemaString == null || jsonSchemaString.isEmpty()) {
                        throw new SchemaValidationException("JSON Schema is not configured");
                    }

                    // Parse the JSON Schema
                    JsonSchema schema = schemaFactory.getSchema(jsonSchemaString);

                    // Validate the payload
                    Set<ValidationMessage> errors = schema.validate(payload);

                    if (!errors.isEmpty()) {
                        String errorMessage = errors.stream()
                                .map(ValidationMessage::getMessage)
                                .collect(Collectors.joining(", "));
                        throw new SchemaValidationException("JSON Schema validation failed: " + errorMessage);
                    }

                    return payload;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}

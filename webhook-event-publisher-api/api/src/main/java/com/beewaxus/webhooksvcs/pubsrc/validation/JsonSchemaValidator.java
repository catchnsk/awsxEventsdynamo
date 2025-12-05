package com.beewaxus.webhooksvcs.pubsrc.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Set;
import java.util.stream.Collectors;

@Component
public class JsonSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(JsonSchemaValidator.class);
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

                    log.info("Validating payload against JSON Schema. Payload: {}, Schema length: {}", 
                            payload.toString(), jsonSchemaString.length());
                    log.debug("Full JSON Schema: {}", jsonSchemaString);

                    // Parse the JSON Schema
                    JsonSchema schema = schemaFactory.getSchema(jsonSchemaString);

                    // Validate the payload
                    Set<ValidationMessage> errors = schema.validate(payload);

                    if (!errors.isEmpty()) {
                        String errorMessage = errors.stream()
                                .map(msg -> {
                                    String path = msg.getInstanceLocation().toString();
                                    String schemaPath = msg.getSchemaLocation().toString();
                                    String message = msg.getMessage();
                                    return String.format("%s at path '%s' (schema path: '%s')", 
                                            message, path, schemaPath);
                                })
                                .collect(Collectors.joining("; "));
                        
                        log.error("JSON Schema validation failed. Number of errors: {}, Errors: {}", errors.size(), errorMessage);
                        log.error("Payload that failed validation: {}", payload.toString());
                        log.error("Schema used for validation (first 1000 chars): {}", 
                                jsonSchemaString.length() > 1000 ? jsonSchemaString.substring(0, 1000) + "..." : jsonSchemaString);
                        
                        // Log each error individually for better debugging
                        errors.forEach(msg -> {
                            log.error("Validation error - Message: {}, Instance Path: {}, Schema Path: {}, Arguments: {}", 
                                    msg.getMessage(), msg.getInstanceLocation(), msg.getSchemaLocation(), msg.getArguments());
                        });
                        
                        throw new SchemaValidationException("JSON Schema validation failed: " + errorMessage);
                    }

                    log.debug("JSON Schema validation successful");
                    return payload;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}

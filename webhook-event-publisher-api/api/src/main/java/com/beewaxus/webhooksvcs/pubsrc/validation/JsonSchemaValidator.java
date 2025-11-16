package com.beewaxus.webhooksvcs.pubsrc.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.beewaxus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Set;

@Component
public class JsonSchemaValidator {

    private final JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);

    public Mono<Void> validate(JsonNode payload, SchemaDefinition schemaDefinition) {
        return Mono.fromCallable(() -> {
                    JsonSchema schema = schemaFactory.getSchema(schemaDefinition.jsonSchema());
                    Set<ValidationMessage> validationMessages = schema.validate(payload);
                    if (!validationMessages.isEmpty()) {
                        throw new SchemaValidationException(validationMessages.toString());
                    }
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }
}

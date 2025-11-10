package com.webhooks.validation.schema;

import reactor.core.publisher.Mono;

public interface SchemaService {

    Mono<SchemaDefinition> fetchSchema(SchemaReference reference);
}

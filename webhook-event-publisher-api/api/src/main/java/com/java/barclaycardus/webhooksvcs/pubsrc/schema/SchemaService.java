package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

import reactor.core.publisher.Mono;

public interface SchemaService {

    Mono<SchemaDefinition> fetchSchema(SchemaReference reference);
}

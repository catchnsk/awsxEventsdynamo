package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

public interface SchemaService {

    Mono<SchemaDefinition> fetchSchema(SchemaReference reference);
    
    Flux<SchemaDefinition> fetchAllSchemas();
    
    Mono<SchemaDetailResponse> fetchSchemaBySchemaId(String schemaId);
}

package com.barclaycardus.webhooksvcs.evntsrc.schema;

import org.springframework.stereotype.Service;

@Service
public class SchemaService {

    private final SchemaRepository repository;
    private final SchemaCache cache;

    public SchemaService(SchemaRepository repository, SchemaCache cache) {
        this.repository = repository;
        this.cache = cache;
    }

    public SchemaMetadata resolve(String schemaId) {
        return cache.get(schemaId, () -> repository.findById(schemaId));
    }
}

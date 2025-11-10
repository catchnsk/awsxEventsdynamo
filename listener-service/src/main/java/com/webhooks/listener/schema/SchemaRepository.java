package com.webhooks.listener.schema;

public interface SchemaRepository {
    SchemaMetadata findById(String schemaId);
}

package com.barclaycardus.webhooksvcs.evntsrc.schema;

public interface SchemaRepository {
    SchemaMetadata findById(String schemaId);
}

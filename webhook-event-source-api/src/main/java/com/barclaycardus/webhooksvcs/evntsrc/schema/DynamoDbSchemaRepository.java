package com.barclaycardus.webhooksvcs.evntsrc.schema;

import com.barclaycardus.webhooksvcs.evntsrc.config.ListenerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import java.util.Locale;
import java.util.Map;

@Repository
public class DynamoDbSchemaRepository implements SchemaRepository {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbSchemaRepository.class);

    private final DynamoDbClient dynamoDbClient;
    private final ListenerProperties properties;

    public DynamoDbSchemaRepository(DynamoDbClient dynamoDbClient, ListenerProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.properties = properties;
    }

    @Override
    public SchemaMetadata findById(String schemaId) {
        GetItemRequest request = GetItemRequest.builder()
                .tableName(properties.getAws().getDynamoDb().getTable())
                .key(Map.of("schema_id", AttributeValue.builder().s(schemaId).build()))
                .build();
        Map<String, AttributeValue> item = dynamoDbClient.getItem(request).item();
        if (item == null || item.isEmpty()) {
            throw new IllegalArgumentException("Schema not found for id " + schemaId);
        }
        SchemaMetadata metadata = toSchemaMetadata(item);
        log.debug("Loaded schema {}", metadata);
        return metadata;
    }

    private SchemaMetadata toSchemaMetadata(Map<String, AttributeValue> item) {
        String schemaId = stringValue(item, "schema_id");
        String schemaDefinition = stringValue(item, "schema_definition");
        SchemaMetadata.FormatType formatType = SchemaMetadata.FormatType.valueOf(stringValue(item, "format_type").toUpperCase(Locale.ROOT));
        boolean transform = "Y".equalsIgnoreCase(stringValue(item, "transform_flag", "N"));
        SchemaMetadata.Status status = SchemaMetadata.Status.valueOf(stringValue(item, "status", "ACTIVE").toUpperCase(Locale.ROOT));
        String producerDomain = stringValue(item, "producer_domain", "default");
        String eventName = stringValue(item, "event_name", "event");
        return new SchemaMetadata(schemaId, schemaDefinition, formatType, transform, status, producerDomain, eventName);
    }

    private String stringValue(Map<String, AttributeValue> item, String key) {
        return stringValue(item, key, null);
    }

    private String stringValue(Map<String, AttributeValue> item, String key, String fallback) {
        AttributeValue value = item.get(key);
        if (value != null && value.s() != null) {
            return value.s();
        }
        if (fallback != null) {
            return fallback;
        }
        throw new IllegalArgumentException("Missing attribute " + key);
    }
}

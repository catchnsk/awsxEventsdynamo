package com.webhooks.listener.schema;

import com.webhooks.listener.config.ListenerProperties;
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
        String schemaId = item.get("schema_id").s();
        String schemaDefinition = item.get("schema_definition").s();
        SchemaMetadata.FormatType formatType = SchemaMetadata.FormatType.valueOf(item.get("format_type").s().toUpperCase(Locale.ROOT));
        boolean transform = "Y".equalsIgnoreCase(item.getOrDefault("transform_flag", AttributeValue.builder().s("N").build()).s());
        SchemaMetadata.Status status = SchemaMetadata.Status.valueOf(item.getOrDefault("status", AttributeValue.builder().s("ACTIVE").build()).s().toUpperCase(Locale.ROOT));
        String producerDomain = item.getOrDefault("producer_domain", AttributeValue.builder().s("default").build()).s();
        String eventName = item.getOrDefault("event_name", AttributeValue.builder().s("event").build()).s();
        return new SchemaMetadata(schemaId, schemaDefinition, formatType, transform, status, producerDomain, eventName);
    }
}

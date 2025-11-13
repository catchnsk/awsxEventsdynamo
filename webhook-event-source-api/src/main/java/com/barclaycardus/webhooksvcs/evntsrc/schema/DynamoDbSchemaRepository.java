package com.barclaycardus.webhooksvcs.evntsrc.schema;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.barclaycardus.webhooksvcs.evntsrc.config.ListenerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.Locale;
import java.util.Map;

@Repository
public class DynamoDbSchemaRepository implements SchemaRepository {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbSchemaRepository.class);

    private final AmazonDynamoDB dynamoDbClient;
    private final ListenerProperties properties;

    public DynamoDbSchemaRepository(AmazonDynamoDB dynamoDbClient, ListenerProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.properties = properties;
    }

    @Override
    public SchemaMetadata findById(String schemaId) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(properties.getAws().getDynamoDb().getTable())
                .withKey(Map.of("schema_id", new AttributeValue().withS(schemaId)));
        Map<String, AttributeValue> item = dynamoDbClient.getItem(request).getItem();
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
        if (value != null && value.getS() != null) {
            return value.getS();
        }
        if (fallback != null) {
            return fallback;
        }
        throw new IllegalArgumentException("Missing attribute " + key);
    }
}

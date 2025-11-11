package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.java.barclaycardus.webhooksvcs.pubsrc.config.WebhooksProperties;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.Map;

@Service
public class DynamoSchemaService implements SchemaService {

    private final AmazonDynamoDB dynamoDbClient;
    private final WebhooksProperties properties;

    public DynamoSchemaService(AmazonDynamoDB dynamoDbClient, WebhooksProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.properties = properties;
    }

    @Override
    public Mono<SchemaDefinition> fetchSchema(SchemaReference reference) {
        return Mono.fromCallable(() -> {
                    Map<String, AttributeValue> item = dynamoDbClient.getItem(new GetItemRequest()
                                    .withTableName(properties.dynamodb().tableName())
                                    .withKey(Map.of("PK", new AttributeValue().withS(reference.partitionKey()))))
                            .getItem();
                    if (item == null || item.isEmpty()) {
                        return null;
                    }
                    return new SchemaDefinition(
                            reference,
                            stringValue(item, "schema", "{}"),
                            "ACTIVE".equals(stringValue(item, "status", "INACTIVE")),
                            Instant.ofEpochMilli(Long.parseLong(numberValue(item, "updated_at", "0")))
                    );
                })
                .subscribeOn(Schedulers.boundedElastic())
                .filter(SchemaDefinition::active);
    }

    private String stringValue(Map<String, AttributeValue> item, String key, String fallback) {
        AttributeValue value = item.get(key);
        if (value != null && value.getS() != null) {
            return value.getS();
        }
        return fallback;
    }

    private String numberValue(Map<String, AttributeValue> item, String key, String fallback) {
        AttributeValue value = item.get(key);
        if (value != null && value.getN() != null) {
            return value.getN();
        }
        return fallback;
    }
}

package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

import com.java.barclaycardus.webhooksvcs.pubsrc.config.WebhooksProperties;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import java.time.Instant;
import java.util.Map;

@Service
public class DynamoSchemaService implements SchemaService {

    private final DynamoDbClient dynamoDbClient;
    private final WebhooksProperties properties;

    public DynamoSchemaService(DynamoDbClient dynamoDbClient, WebhooksProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.properties = properties;
    }

    @Override
    public Mono<SchemaDefinition> fetchSchema(SchemaReference reference) {
        return Mono.fromCallable(() -> {
                    Map<String, AttributeValue> item = dynamoDbClient.getItem(GetItemRequest.builder()
                            .tableName(properties.dynamodb().tableName())
                            .key(Map.of(
                                    "PK", AttributeValue.builder().s(reference.partitionKey()).build(),
                                    "SK", AttributeValue.builder().s(reference.sortKey()).build()))
                            .build()).item();

                    if (item == null || item.isEmpty()) {
                        return null;
                    }
                    return new SchemaDefinition(
                            reference,
                            stringValue(item, "EVENT_SCHEMA_DEFINITION_JSON", "{}"),
                            stringValue(item, "EVENT_SCHEMA_DEFINITION_XML", null),
                            stringValue(item, "EVENT_SCHEMA_DEFINITION_AVRO", null),
                            "ACTIVE".equals(stringValue(item, "EVENT_SCHEMA_STATUS", "INACTIVE")),
                            Instant.parse(stringValue(item, "UPDATE_TS", Instant.now().toString()))
                    );
                })
                .subscribeOn(Schedulers.boundedElastic())
                .filter(SchemaDefinition::active);
    }

    private String stringValue(Map<String, AttributeValue> item, String key, String fallback) {
        AttributeValue value = item.get(key);
        if (value != null && value.s() != null) {
            return value.s();
        }
        return fallback;
    }
}

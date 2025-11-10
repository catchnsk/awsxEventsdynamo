package com.webhooks.validation.schema;

import com.webhooks.validation.config.WebhooksProperties;
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
                                    .key(Map.of("PK", AttributeValue.builder().s(reference.partitionKey()).build()))
                                    .build())
                            .item();
                    if (item == null || item.isEmpty()) {
                        return null;
                    }
                    return new SchemaDefinition(
                            reference,
                            item.getOrDefault("schema", AttributeValue.fromS("{}")).s(),
                            "ACTIVE".equals(item.getOrDefault("status", AttributeValue.fromS("INACTIVE")).s()),
                            Instant.ofEpochMilli(Long.parseLong(item.getOrDefault("updated_at", AttributeValue.fromN("0")).n()))
                    );
                })
                .subscribeOn(Schedulers.boundedElastic())
                .filter(SchemaDefinition::active);
    }
}

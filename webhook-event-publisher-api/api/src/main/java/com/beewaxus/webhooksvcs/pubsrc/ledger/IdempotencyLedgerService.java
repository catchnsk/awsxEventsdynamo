package com.beewaxus.webhooksvcs.pubsrc.ledger;

import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.core.exception.SdkException;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Service
public class IdempotencyLedgerService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyLedgerService.class);

    private final DynamoDbClient dynamoDbClient;
    private final WebhooksProperties properties;

    public IdempotencyLedgerService(DynamoDbClient dynamoDbClient, WebhooksProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.properties = properties;
    }

    public Mono<Void> recordEventStatus(String eventId, EventStatus status, String schemaId) {
        return Mono.fromCallable(() -> {
                    try {
                        String tableName = properties.dynamodb().idempotencyLedgerTableName();
                        Instant now = Instant.now();
                        String timestamp = now.toString();

                        Map<String, AttributeValue> item = new HashMap<>();
                        item.put("eventId", AttributeValue.builder().s(eventId).build());
                        item.put("eventStatus", AttributeValue.builder().s(status.getValue()).build());
                        item.put("CREATION_TIMESTAMP", AttributeValue.builder().s(timestamp).build());
                        item.put("UPDATED_TIMESTAMP", AttributeValue.builder().s(timestamp).build());
                        
                        if (schemaId != null && !schemaId.isEmpty()) {
                            item.put("EVENT_SCHEMAID", AttributeValue.builder().s(schemaId).build());
                        }

                        PutItemRequest putRequest = PutItemRequest.builder()
                                .tableName(tableName)
                                .item(item)
                                .build();

                        dynamoDbClient.putItem(putRequest);
                        log.debug("Recorded event status: eventId={}, status={}, schemaId={}", 
                                eventId, status, schemaId);
                        return null;
                    } catch (ResourceNotFoundException e) {
                        log.error("DynamoDB table '{}' not found for idempotency ledger", 
                                properties.dynamodb().idempotencyLedgerTableName(), e);
                        throw new IdempotencyLedgerException("Idempotency ledger table does not exist", e);
                    } catch (SdkException e) {
                        log.error("DynamoDB error while recording event status for eventId {}: {}", 
                                eventId, e.getMessage(), e);
                        throw new IdempotencyLedgerException("Failed to record event status: " + e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    public enum EventStatus {
        EVENT_READY_FOR_PROCESSING("EVENT_READY_FOR_PROCESSING"),
        EVENT_READY_FOR_DELIVERY("EVENT_READY_FOR_DELIVERY"),
        EVENT_DELIVERED("EVENT_DELIVERED"),
        EVENT_PROCESSING_FAILED("EVENT_PROCESSING_FAILED"),
        EVENT_DELIVERY_FAILED("EVENT_DELIVERY_FAILED");

        private final String value;

        EventStatus(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}


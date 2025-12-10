package com.beewaxus.webhooksvcs.pubsrc.schema;

import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.core.exception.SdkException;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

@Service
public class DynamoSchemaService implements SchemaService {

    private static final Logger log = LoggerFactory.getLogger(DynamoSchemaService.class);

    private final DynamoDbClient dynamoDbClient;
    private final WebhooksProperties properties;

    public DynamoSchemaService(
            DynamoDbClient dynamoDbClient,
            WebhooksProperties properties) {
        this.dynamoDbClient = dynamoDbClient;
        this.properties = properties;
    }

    @Override
    public Mono<SchemaDefinition> fetchSchema(SchemaReference reference) {
        return Mono.fromCallable(() -> {
                    try {
                        // First try to query by PK/SK format (if data follows expected format)
                        Map<String, AttributeValue> item = null;
                        try {
                            item = dynamoDbClient.getItem(GetItemRequest.builder()
                                    .tableName(properties.dynamodb().tableName())
                                    .key(Map.of(
                                            "PK", AttributeValue.builder().s(reference.partitionKey()).build(),
                                            "SK", AttributeValue.builder().s(reference.sortKey()).build()))
                                    .build()).item();
                        } catch (Exception e) {
                            log.debug("GetItem by PK/SK failed, trying scan with filter: {}", e.getMessage());
                        }

                        // If GetItem didn't return a result, try scanning with filter expression
                        if (item == null || item.isEmpty()) {
                            log.debug("Querying schema by attributes: domain={}, event={}, version={}",
                                    reference.domain(), reference.eventName(), reference.version());

                            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                            expressionAttributeValues.put(":domain", AttributeValue.builder().s(reference.domain()).build());
                            expressionAttributeValues.put(":eventName", AttributeValue.builder().s(reference.eventName()).build());
                            expressionAttributeValues.put(":version", AttributeValue.builder().s(reference.version()).build());

                            ScanRequest scanRequest = ScanRequest.builder()
                                    .tableName(properties.dynamodb().tableName())
                                    .filterExpression("PRODUCER_DOMAIN = :domain AND EVENT_NAME = :eventName AND VERSION = :version")
                                    .expressionAttributeValues(expressionAttributeValues)
                                    .build();

                            ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

                            if (scanResponse.items().isEmpty()) {
                                log.debug("No schema found for domain={}, event={}, version={}",
                                        reference.domain(), reference.eventName(), reference.version());
                                return null;
                            }

                            // Use the first matching item
                            item = scanResponse.items().get(0);
                            log.debug("Found schema via scan for domain={}, event={}, version={}",
                                    reference.domain(), reference.eventName(), reference.version());
                        }

                        if (item == null || item.isEmpty()) {
                            return null;
                        }

                        // Read both schema definitions
                        String jsonSchema = stringValue(item, "EVENT_SCHEMA_DEFINITION", null);
                        String avroSchema = stringValue(item, "EVENT_SCHEMA_DEFINITION_AVRO", null);

                        // Determine schema format type
                        SchemaFormatType formatType;
                        if (jsonSchema != null && !jsonSchema.isEmpty()) {
                            formatType = SchemaFormatType.JSON_SCHEMA;
                        } else if (avroSchema != null && !avroSchema.isEmpty()) {
                            formatType = SchemaFormatType.AVRO_SCHEMA;
                        } else {
                            return null; // No schema available
                        }

                        return new SchemaDefinition(
                                reference,
                                jsonSchema,
                                avroSchema,
                                formatType,
                                "ACTIVE".equals(stringValue(item, "EVENT_SCHEMA_STATUS", "INACTIVE")),
                                Instant.parse(stringValue(item, "UPDATE_TS", Instant.now().toString())),
                                stringValue(item, "EVENT_SCHEMA_ID", null)
                        );
                    } catch (ResourceNotFoundException e) {
                        log.error("DynamoDB table '{}' not found", properties.dynamodb().tableName(), e);
                        throw new DynamoDbException("DynamoDB table '" + properties.dynamodb().tableName() + "' does not exist", e);
                    } catch (SdkException e) {
                        log.error("DynamoDB error while fetching schema for {}: {}", reference, e.getMessage(), e);
                        throw new DynamoDbException("DynamoDB service unavailable: " + e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .filter(SchemaDefinition::active)
                .onErrorMap(SdkException.class, e -> {
                    if (e instanceof ResourceNotFoundException) {
                        return new DynamoDbException("DynamoDB table '" + properties.dynamodb().tableName() + "' does not exist", e);
                    }
                    return new DynamoDbException("DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    @Override
    public Flux<SchemaDefinition> fetchAllSchemas() {
        return Mono.fromCallable(() -> {
                    try {
                        String tableName = properties.dynamodb().tableName();
                        log.debug("Scanning DynamoDB table: {}", tableName);

                        ScanResponse scanResponse = dynamoDbClient.scan(ScanRequest.builder()
                                .tableName(tableName)
                                .build());

                        log.debug("Scan completed. Found {} items in table {}",
                                scanResponse.items().size(), tableName);
                        return scanResponse;
                    } catch (ResourceNotFoundException e) {
                        log.error("DynamoDB table '{}' not found. Error details: {}",
                                properties.dynamodb().tableName(), e.getMessage(), e);
                        throw new DynamoDbException("DynamoDB table '" + properties.dynamodb().tableName() + "' does not exist", e);
                    } catch (SdkException e) {
                        log.error("DynamoDB error while fetching all schemas from table '{}': {}",
                                properties.dynamodb().tableName(), e.getMessage(), e);
                        throw new DynamoDbException("DynamoDB service unavailable: " + e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(scanResponse -> Flux.fromIterable(scanResponse.items()))
                .map(this::itemToSchemaDefinition)
                .filter(item -> item != null) // Filter out null items (invalid formats)
                .filter(SchemaDefinition::active)
                .onErrorMap(SdkException.class, e -> {
                    if (e instanceof ResourceNotFoundException) {
                        return new DynamoDbException("DynamoDB table '" + properties.dynamodb().tableName() + "' does not exist", e);
                    }
                    return new DynamoDbException("DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    @Override
    public Mono<SchemaDetailResponse> fetchSchemaBySchemaId(String schemaId) {
        return Mono.fromCallable(() -> {
                    try {
                        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                        expressionAttributeValues.put(":schemaId", AttributeValue.builder().s(schemaId).build());

                        ScanRequest scanRequest = ScanRequest.builder()
                                .tableName(properties.dynamodb().tableName())
                                .filterExpression("EVENT_SCHEMA_ID = :schemaId")
                                .expressionAttributeValues(expressionAttributeValues)
                                .build();

                        ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

                        if (scanResponse.items().isEmpty()) {
                            return null;
                        }

                        // Return the first matching item (assuming EVENT_SCHEMA_ID is unique)
                        return scanResponse.items().get(0);
                    } catch (ResourceNotFoundException e) {
                        log.error("DynamoDB table '{}' not found while fetching schemaId: {}", properties.dynamodb().tableName(), schemaId, e);
                        throw new DynamoDbException("DynamoDB table '" + properties.dynamodb().tableName() + "' does not exist", e);
                    } catch (SdkException e) {
                        log.error("DynamoDB error while fetching schema by schemaId {}: {}", schemaId, e.getMessage(), e);
                        throw new DynamoDbException("DynamoDB service unavailable: " + e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(this::itemToSchemaDetailResponse)
                .onErrorMap(SdkException.class, e -> {
                    if (e instanceof ResourceNotFoundException) {
                        return new DynamoDbException("DynamoDB table '" + properties.dynamodb().tableName() + "' does not exist", e);
                    }
                    return new DynamoDbException("DynamoDB service unavailable: " + e.getMessage(), e);
                });
    }

    private String stringValue(Map<String, AttributeValue> item, String key, String fallback) {
        AttributeValue value = item.get(key);
        if (value != null && value.s() != null) {
            return value.s();
        }
        return fallback;
    }

    private SchemaDefinition itemToSchemaDefinition(Map<String, AttributeValue> item) {
        try {
            String pk = stringValue(item, "PK", "");
            String sk = stringValue(item, "SK", "");

            SchemaReference reference = null;

            // Try to parse from PK/SK format first: "SCHEMA#{domain}#{eventName}" / "v{version}"
            if (pk.startsWith("SCHEMA#") && sk.startsWith("v")) {
                reference = parseSchemaReferenceFromPkSk(pk, sk);
            } else {
                // Fallback: Try to read from direct attributes
                String domain = stringValue(item, "PRODUCER_DOMAIN", null);
                String eventName = stringValue(item, "EVENT_NAME", null);
                String version = stringValue(item, "VERSION", null);

                if (domain != null && eventName != null && version != null) {
                    reference = new SchemaReference(domain, eventName, version);
                } else {
                    // If neither format works, return null (will be filtered out)
                    return null;
                }
            }

            // Read both schema definitions
            String jsonSchema = stringValue(item, "EVENT_SCHEMA_DEFINITION", null);
            String avroSchema = stringValue(item, "EVENT_SCHEMA_DEFINITION_AVRO", null);

            // Determine schema format type
            SchemaFormatType formatType;
            if (jsonSchema != null && !jsonSchema.isEmpty()) {
                formatType = SchemaFormatType.JSON_SCHEMA;
            } else if (avroSchema != null && !avroSchema.isEmpty()) {
                formatType = SchemaFormatType.AVRO_SCHEMA;
            } else {
                return null; // No schema available
            }

            String status = stringValue(item, "EVENT_SCHEMA_STATUS", "INACTIVE");
            String updateTsStr = stringValue(item, "UPDATE_TS", null);
            boolean isActive = "ACTIVE".equals(status);

            // Parse UPDATE_TS with error handling
            Instant updateTs = Instant.now(); // Default to now if parsing fails
            if (updateTsStr != null && !updateTsStr.isEmpty()) {
                try {
                    updateTs = Instant.parse(updateTsStr);
                } catch (DateTimeParseException e) {
                    log.warn("Failed to parse UPDATE_TS: {} for PK: {}, SK: {}, using current time", updateTsStr, pk, sk);
                }
            }

            // Extract EVENT_SCHEMA_ID from DynamoDB item
            String eventSchemaId = stringValue(item, "EVENT_SCHEMA_ID", null);

            return new SchemaDefinition(
                    reference,
                    jsonSchema,
                    avroSchema,
                    formatType,
                    isActive,
                    updateTs,
                    eventSchemaId
            );
        } catch (Exception e) {
            // Log and skip invalid items instead of throwing
            String pk = stringValue(item, "PK", "N/A");
            String sk = stringValue(item, "SK", "N/A");
            log.error("Error converting DynamoDB item to SchemaDefinition (PK: {}, SK: {}): {}", pk, sk, e.getMessage(), e);
            return null;
        }
    }

    private SchemaReference parseSchemaReferenceFromPkSk(String pk, String sk) {
        // PK format: "SCHEMA#{domain}#{eventName}"
        // SK format: "v{version}"
        if (!pk.startsWith("SCHEMA#")) {
            throw new IllegalArgumentException("Invalid PK format: " + pk);
        }

        String[] pkParts = pk.substring(7).split("#", 2); // Remove "SCHEMA#" prefix
        if (pkParts.length != 2) {
            throw new IllegalArgumentException("Invalid PK format: " + pk);
        }

        String domain = pkParts[0];
        String eventName = pkParts[1];

        if (!sk.startsWith("v")) {
            throw new IllegalArgumentException("Invalid SK format: " + sk);
        }
        String version = sk.substring(1); // Remove "v" prefix

        return new SchemaReference(domain, eventName, version);
    }

    private SchemaDetailResponse itemToSchemaDetailResponse(Map<String, AttributeValue> item) {
        try {
            String insertTsStr = stringValue(item, "INSERT_TS", null);
            String updateTsStr = stringValue(item, "UPDATE_TS", null);

            Instant insertTs = null;
            Instant updateTs = null;

            // Parse timestamps with error handling
            if (insertTsStr != null && !insertTsStr.isEmpty()) {
                try {
                    insertTs = Instant.parse(insertTsStr);
                } catch (DateTimeParseException e) {
                    log.warn("Failed to parse INSERT_TS: {}", insertTsStr, e);
                }
            }

            if (updateTsStr != null && !updateTsStr.isEmpty()) {
                try {
                    updateTs = Instant.parse(updateTsStr);
                } catch (DateTimeParseException e) {
                    log.warn("Failed to parse UPDATE_TS: {}", updateTsStr, e);
                }
            }

            return new SchemaDetailResponse(
                    stringValue(item, "EVENT_SCHEMA_ID", null),
                    stringValue(item, "PRODUCER_DOMAIN", null),
                    stringValue(item, "EVENT_NAME", null),
                    stringValue(item, "VERSION", null),
                    stringValue(item, "EVENT_SCHEMA_HEADER", null),
                    stringValue(item, "EVENT_SCHEMA_DEFINITION_AVRO", null),
                    stringValue(item, "EVENT_SCHEMA_DEFINITION", null),
                    stringValue(item, "EVENT_SAMPLE", null),
                    stringValue(item, "EVENT_SCHEMA_STATUS", null),
                    stringValue(item, "HAS_SENSITIVE_DATA", null),
                    stringValue(item, "PRODUCER_SYSTEM_USERS_ID", null),
                    stringValue(item, "TOPIC_NAME", null),
                    stringValue(item, "TOPIC_STATUS", null),
                    insertTs,
                    stringValue(item, "INSERT_USER", null),
                    updateTs,
                    stringValue(item, "UPDATE_USER", null)
            );
        } catch (Exception e) {
            log.error("Error converting DynamoDB item to SchemaDetailResponse", e);
            throw new RuntimeException("Failed to convert schema item: " + e.getMessage(), e);
        }
    }
}
package com.beewaxus.webhooksvcs.pubsrc.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

@Configuration
public class AwsConfig {

    private static final Logger log = LoggerFactory.getLogger(AwsConfig.class);

    @Value("${aws.dynamodb.endpoint:}")
    private String dynamoDbEndpoint;

    @Value("${aws.region:us-east-1}")
    private String awsRegion;

    @Bean
    public DynamoDbClient dynamoDbClient() {
        log.info("Configuring DynamoDB client with endpoint: '{}' and region: {}", dynamoDbEndpoint, awsRegion);

        var builder = DynamoDbClient.builder();

        if (dynamoDbEndpoint != null && !dynamoDbEndpoint.isEmpty()) {
            log.info("Using local DynamoDB configuration with endpoint: {}", dynamoDbEndpoint);
            try {
                // Local DynamoDB configuration
                URI endpointUri = URI.create(dynamoDbEndpoint);
                log.info("Parsed endpoint URI: {} (scheme: {}, host: {}, port: {})", 
                        endpointUri, endpointUri.getScheme(), endpointUri.getHost(), endpointUri.getPort());
                
                builder.endpointOverride(endpointUri)
                        .credentialsProvider(StaticCredentialsProvider.create(
                                AwsBasicCredentials.create("dummy", "dummy")))
                        .region(Region.US_EAST_1); // Must specify region even for local
                
                DynamoDbClient client = builder.build();
                log.info("DynamoDB client successfully configured with local endpoint: {}", endpointUri);
                
                // Test connection by listing tables (non-blocking, just for logging)
                try {
                    var tables = client.listTables();
                    log.info("Successfully connected to DynamoDB Local. Found {} tables: {}", 
                            tables.tableNames().size(), tables.tableNames());
                } catch (Exception e) {
                    log.warn("Could not verify connection to DynamoDB Local at {} (this may be normal if DynamoDB is starting up): {}", 
                            endpointUri, e.getMessage());
                    log.debug("Connection test exception details", e);
                }
                
                return client;
            } catch (Exception e) {
                log.error("Error configuring local DynamoDB client: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to configure DynamoDB client", e);
            }
        } else {
            log.info("Using AWS DynamoDB configuration (no endpoint override)");
            // AWS DynamoDB configuration
            builder.region(Region.of(awsRegion))
                    .credentialsProvider(DefaultCredentialsProvider.create());
            return builder.build();
        }
    }
}

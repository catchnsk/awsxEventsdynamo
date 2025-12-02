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
        log.info("Configuring DynamoDB client with endpoint: {} and region: {}", dynamoDbEndpoint, awsRegion);

        var builder = DynamoDbClient.builder()
                .region(Region.of(awsRegion));

        if (dynamoDbEndpoint != null && !dynamoDbEndpoint.isEmpty()) {
            log.info("Using local DynamoDB configuration with endpoint: {}", dynamoDbEndpoint);
            // Local DynamoDB configuration - disable path style access
            URI endpointUri = URI.create(dynamoDbEndpoint);
            builder.endpointOverride(endpointUri)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("dummy", "dummy")))
                    .region(Region.US_EAST_1); // Must specify region even for local
            log.info("DynamoDB client configured with URI: {}", endpointUri);
        } else {
            log.info("Using AWS DynamoDB configuration");
            // AWS DynamoDB configuration
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }
}

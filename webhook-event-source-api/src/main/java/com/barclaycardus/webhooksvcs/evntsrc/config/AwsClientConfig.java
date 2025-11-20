package com.barclaycardus.webhooksvcs.evntsrc.config;


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
public class AwsClientConfig {

    @Value("${aws.dynamodb.endpoint:}")
    private String dynamoDbEndpoint;

    @Value("${aws.region:us-east-1}")
    private String awsRegion;

    @Bean
    public DynamoDbClient dynamoDbClient() {
        var builder = DynamoDbClient.builder()
                .region(Region.of(awsRegion));

        if (dynamoDbEndpoint != null && !dynamoDbEndpoint.isEmpty()) {
            // Local DynamoDB configuration
            builder.endpointOverride(URI.create(dynamoDbEndpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("dummy", "dummy")));
        } else {
            // AWS DynamoDB configuration
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }
}
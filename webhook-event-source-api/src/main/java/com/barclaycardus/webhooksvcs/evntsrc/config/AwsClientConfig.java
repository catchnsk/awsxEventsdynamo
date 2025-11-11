package com.barclaycardus.webhooksvcs.evntsrc.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration
public class AwsClientConfig {

    @Bean
    public DynamoDbClient dynamoDbClient(ListenerProperties properties) {
        return DynamoDbClient.builder()
                .region(Region.of(properties.getAws().getRegion()))
                .build();
    }
}

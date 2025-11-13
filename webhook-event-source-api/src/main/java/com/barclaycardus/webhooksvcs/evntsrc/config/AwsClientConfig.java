package com.barclaycardus.webhooksvcs.evntsrc.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AwsClientConfig {

    @Bean
    public AmazonDynamoDB dynamoDbClient(ListenerProperties properties) {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(properties.getAws().getRegion())
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }
}

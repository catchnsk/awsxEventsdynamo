package com.webhooks.reference;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ReferenceProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReferenceProducerApplication.class, args);
    }
}

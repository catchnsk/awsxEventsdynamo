package com.webhooks.listener;

import com.webhooks.listener.config.ListenerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.jms.annotation.EnableJms;

@SpringBootApplication
@EnableConfigurationProperties(ListenerProperties.class)
@EnableJms
public class ListenerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ListenerServiceApplication.class, args);
    }
}

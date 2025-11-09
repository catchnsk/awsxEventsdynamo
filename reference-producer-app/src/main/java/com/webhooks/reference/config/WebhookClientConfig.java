package com.webhooks.reference.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.sdk.core.SchemaCache;
import com.webhooks.sdk.core.SchemaDefinition;
import com.webhooks.sdk.core.SchemaReference;
import com.webhooks.sdk.core.SchemaRegistryClient;
import com.webhooks.sdk.core.WebhookClient;
import com.webhooks.sdk.http.HttpWebhookPublisher;
import com.webhooks.sdk.http.TokenProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Configuration
public class WebhookClientConfig {

    @Bean
    public WebhookClient webhookClient(WebhookProducerProperties properties,
                                       ResourceLoader resourceLoader,
                                       ObjectMapper objectMapper) throws IOException {
        SchemaReference reference = SchemaReference.of(
                properties.domain(),
                properties.eventName(),
                properties.schemaVersion());

        String schema = resourceLoader.getResource("classpath:schemas/customer-updated.json")
                .getContentAsString(StandardCharsets.UTF_8);

        SchemaRegistryClient.SchemaFetcher fetcher = ref -> new SchemaDefinition(ref, schema, Instant.now());

        HttpWebhookPublisher transport = HttpWebhookPublisher.builder()
                .endpoint(properties.endpoint())
                .environment("dev")
                .tokenProvider(TokenProvider.staticToken(properties.token()))
                .objectMapper(objectMapper)
                .build();

        return WebhookClient.builder()
                .schemaFetcher(fetcher)
                .schemaCache(SchemaCache.inMemory(1_000))
                .transport(transport)
                .build();
    }
}

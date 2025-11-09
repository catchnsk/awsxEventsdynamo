package com.webhooks.sdk.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.webhooks.sdk.core.Event;
import com.webhooks.sdk.core.SchemaDefinition;
import com.webhooks.sdk.core.SchemaReference;
import com.webhooks.sdk.core.SchemaRegistryClient;
import com.webhooks.sdk.core.WebhookClient;
import com.webhooks.sdk.http.HttpWebhookPublisher;
import com.webhooks.sdk.http.TokenProvider;

import java.time.Instant;
import java.util.Map;

public final class DemoApplication {

    private DemoApplication() {}

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        SchemaReference schemaReference = SchemaReference.of("demo.producer", "CustomerUpdated", "v1");
        String schemaJson = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "type": "object",
                  "properties": {
                    "customerId": {"type":"string"},
                    "status": {"type":"string"}
                  },
                  "required": ["customerId","status"]
                }
                """;

        var schemaFetcher = SchemaRegistryClient.SchemaFetcher(ref -> new SchemaDefinition(ref, schemaJson, Instant.now()));

        HttpWebhookPublisher transport = HttpWebhookPublisher.builder()
                .endpoint(System.getenv().getOrDefault("WEBHOOK_ENDPOINT", "http://localhost:8080"))
                .environment(System.getenv().getOrDefault("WEBHOOK_ENV", "dev"))
                .tokenProvider(TokenProvider.staticToken(System.getenv().getOrDefault("WEBHOOK_TOKEN", "demo-token")))
                .build();

        WebhookClient client = WebhookClient.builder()
                .schemaFetcher(schemaFetcher)
                .transport(transport)
                .build();

        Event event = Event.builder(schemaReference)
                .payload(mapper.readTree("{\"customerId\":\"C-100\",\"status\":\"ACTIVE\"}"))
                .headers(Map.of("source", "reference-app"))
                .build();

        client.publish(event);
    }
}

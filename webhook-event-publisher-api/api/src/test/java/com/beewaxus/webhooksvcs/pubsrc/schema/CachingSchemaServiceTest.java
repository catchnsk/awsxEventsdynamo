package com.beewaxus.webhooksvcs.pubsrc.schema;

import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CachingSchemaServiceTest {

    private DynamoSchemaService delegate;
    private CachingSchemaService cachingSchemaService;

    @BeforeEach
    void setUp() {
        delegate = Mockito.mock(DynamoSchemaService.class);

        WebhooksProperties properties = new WebhooksProperties(
                new WebhooksProperties.DynamoProperties("event_schema"),
                new WebhooksProperties.KafkaProperties(
                        "localhost:9092",
                        "wh.ingress",
                        Duration.ofSeconds(5),
                        1,
                        Duration.ofSeconds(1)
                ),
                new WebhooksProperties.CacheProperties(
                        true,
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(5),
                        100
                )
        );

        cachingSchemaService = new CachingSchemaService(delegate, properties);
    }

    @Test
    void fetchSchema_UsesCachedValueOnSubsequentCalls() {
        SchemaReference reference = new SchemaReference("demo", "CustomerUpdated", "v1");
        SchemaDefinition schemaDefinition = new SchemaDefinition(reference, null, "{}", SchemaFormatType.AVRO_SCHEMA, true, Instant.now());

        when(delegate.fetchSchema(reference)).thenReturn(Mono.just(schemaDefinition));

        StepVerifier.create(cachingSchemaService.fetchSchema(reference))
                .expectNext(schemaDefinition)
                .verifyComplete();

        StepVerifier.create(cachingSchemaService.fetchSchema(reference))
                .expectNext(schemaDefinition)
                .verifyComplete();

        verify(delegate, times(1)).fetchSchema(reference);
    }

    @Test
    void fetchSchemaBySchemaId_CachesDetailResponses() {
        SchemaDetailResponse detailResponse = new SchemaDetailResponse(
                "SCHEMA_001",
                "demo",
                "CustomerUpdated",
                "v1",
                null,
                "{\"type\":\"record\"}",
                "{\"type\":\"record\"}",
                null,
                "ACTIVE",
                "NO",
                "user",
                "topic",
                "ACTIVE",
                Instant.now(),
                "user",
                Instant.now(),
                "user"
        );

        when(delegate.fetchSchemaBySchemaId(anyString())).thenReturn(Mono.just(detailResponse));

        StepVerifier.create(cachingSchemaService.fetchSchemaBySchemaId("SCHEMA_001"))
                .expectNext(detailResponse)
                .verifyComplete();

        StepVerifier.create(cachingSchemaService.fetchSchemaBySchemaId("SCHEMA_001"))
                .expectNext(detailResponse)
                .verifyComplete();

        verify(delegate, times(1)).fetchSchemaBySchemaId("SCHEMA_001");
    }

    @Test
    void fetchSchema_WhenCacheDisabled_DoesNotUseCache() {
        WebhooksProperties disabledProps = new WebhooksProperties(
                new WebhooksProperties.DynamoProperties("event_schema"),
                new WebhooksProperties.KafkaProperties(
                        "localhost:9092",
                        "wh.ingress",
                        Duration.ofSeconds(5),
                        1,
                        Duration.ofSeconds(1)
                ),
                new WebhooksProperties.CacheProperties(
                        false,
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(5),
                        100
                )
        );

        CachingSchemaService disabledService = new CachingSchemaService(delegate, disabledProps);
        SchemaReference reference = new SchemaReference("demo", "CustomerUpdated", "v1");
        SchemaDefinition schemaDefinition = new SchemaDefinition(reference, null, "{}", SchemaFormatType.AVRO_SCHEMA, true, Instant.now());

        when(delegate.fetchSchema(reference)).thenReturn(Mono.just(schemaDefinition));

        StepVerifier.create(disabledService.fetchSchema(reference))
                .expectNext(schemaDefinition)
                .verifyComplete();

        StepVerifier.create(disabledService.fetchSchema(reference))
                .expectNext(schemaDefinition)
                .verifyComplete();

        verify(delegate, times(2)).fetchSchema(reference);
    }

    @Test
    void evictAndReload_ClearsAndReloadsCache() {
        SchemaReference reference = new SchemaReference("demo", "CustomerUpdated", "v1");
        SchemaDefinition schemaDefinition = new SchemaDefinition(reference, null, "{}", SchemaFormatType.AVRO_SCHEMA, true, Instant.now());

        when(delegate.fetchSchema(reference)).thenReturn(Mono.just(schemaDefinition));
        when(delegate.fetchAllSchemas()).thenReturn(Flux.just(schemaDefinition));

        StepVerifier.create(cachingSchemaService.fetchSchema(reference))
                .expectNext(schemaDefinition)
                .verifyComplete();

        StepVerifier.create(cachingSchemaService.evictAndReload())
                .verifyComplete();

        verify(delegate, times(1)).fetchAllSchemas();
    }
}

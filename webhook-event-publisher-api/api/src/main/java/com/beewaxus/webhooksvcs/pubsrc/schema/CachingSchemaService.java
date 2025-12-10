package com.beewaxus.webhooksvcs.pubsrc.schema;

import com.beewaxus.webhooksvcs.pubsrc.config.WebhooksProperties;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@Primary
public class CachingSchemaService implements SchemaService {

    private static final Logger log = LoggerFactory.getLogger(CachingSchemaService.class);

    private final SchemaService delegate;
    private final Cache<SchemaReference, SchemaDefinition> schemaCache;
    private final Cache<String, SchemaDetailResponse> schemaDetailCache;
    private final boolean cacheEnabled;

    public CachingSchemaService(DynamoSchemaService delegate, WebhooksProperties properties) {
        this.delegate = delegate;

        WebhooksProperties.CacheProperties cacheProps = properties.cache();
        Duration schemaTtl = Duration.ofMinutes(5);
        Duration schemaDetailTtl = Duration.ofMinutes(5);
        int maxEntries = 1000;

        if (cacheProps != null) {
            schemaTtl = cacheProps.getSchemaTtl();
            schemaDetailTtl = cacheProps.getSchemaDetailTtl();
            maxEntries = cacheProps.getMaximumEntries();
        }

        this.cacheEnabled = cacheProps == null || cacheProps.isEnabled();

        if (cacheEnabled) {
            this.schemaCache = Caffeine.newBuilder()
                    .maximumSize(maxEntries)
                    .expireAfterWrite(schemaTtl)
                    .build();

            this.schemaDetailCache = Caffeine.newBuilder()
                    .maximumSize(maxEntries)
                    .expireAfterWrite(schemaDetailTtl)
                    .build();
        } else {
            this.schemaCache = null;
            this.schemaDetailCache = null;
        }
    }

    @Override
    public Mono<SchemaDefinition> fetchSchema(SchemaReference reference) {
        if (!cacheEnabled) {
            log.debug("Cache is disabled, fetching schema directly from DynamoDB for reference: {}", reference);
            return delegate.fetchSchema(reference);
        }

        SchemaDefinition cached = schemaCache.getIfPresent(reference);
        if (cached != null) {
            log.debug("Cache HIT for schema reference: {} (domain={}, eventName={}, version={})",
                    reference, reference.domain(), reference.eventName(), reference.version());
            return Mono.just(cached);
        }

        log.debug("Cache MISS for schema reference: {} (domain={}, eventName={}, version={}). Fetching from DynamoDB...",
                reference, reference.domain(), reference.eventName(), reference.version());
        return delegate.fetchSchema(reference)
                .doOnNext(schemaDefinition -> {
                    schemaCache.put(reference, schemaDefinition);
                    log.debug("Cached schema for reference: {} (domain={}, eventName={}, version={})",
                            reference, reference.domain(), reference.eventName(), reference.version());
                })
                .doOnError(error -> {
                    log.warn("Failed to fetch schema from DynamoDB for reference: {} (domain={}, eventName={}, version={}). Error: {}",
                            reference, reference.domain(), reference.eventName(), reference.version(), error.getMessage());
                });
    }

    @Override
    public Flux<SchemaDefinition> fetchAllSchemas() {
        if (!cacheEnabled) {
            return delegate.fetchAllSchemas();
        }

        return delegate.fetchAllSchemas()
                .doOnNext(schemaDefinition -> schemaCache.put(schemaDefinition.reference(), schemaDefinition));
    }

    @Override
    public Mono<SchemaDetailResponse> fetchSchemaBySchemaId(String schemaId) {
        if (!cacheEnabled) {
            return delegate.fetchSchemaBySchemaId(schemaId);
        }

        SchemaDetailResponse cached = schemaDetailCache.getIfPresent(schemaId);
        if (cached != null) {
            return Mono.just(cached);
        }

        return delegate.fetchSchemaBySchemaId(schemaId)
                .doOnNext(schemaDetail -> {
                    schemaDetailCache.put(schemaId, schemaDetail);
                    boolean hasReferenceData = schemaDetail.producerDomain() != null
                            && schemaDetail.eventName() != null
                            && schemaDetail.version() != null;
                    boolean hasSchemaDefinition = schemaDetail.eventSchemaDefinition() != null
                            || schemaDetail.eventSchemaDefinitionAvro() != null;

                    if (hasReferenceData && hasSchemaDefinition) {
                        SchemaReference reference = new SchemaReference(
                                schemaDetail.producerDomain(),
                                schemaDetail.eventName(),
                                schemaDetail.version()
                        );

                        String jsonSchema = schemaDetail.eventSchemaDefinition();
                        String avroSchema = schemaDetail.eventSchemaDefinitionAvro();

                        // Determine schema format type
                        SchemaFormatType formatType;
                        if (jsonSchema != null && !jsonSchema.isEmpty()) {
                            formatType = SchemaFormatType.JSON_SCHEMA;
                        } else {
                            formatType = SchemaFormatType.AVRO_SCHEMA;
                        }

                        SchemaDefinition definition = new SchemaDefinition(
                                reference,
                                jsonSchema,
                                avroSchema,
                                formatType,
                                "ACTIVE".equals(schemaDetail.eventSchemaStatus()),
                                schemaDetail.updateTs(),
                                schemaDetail.eventSchemaId()
                        );
                        schemaCache.put(reference, definition);
                    }
                });
    }

    @Override
    public Mono<Void> evictAndReload() {
        if (!cacheEnabled) {
            return Mono.empty();
        }

        schemaCache.invalidateAll();
        schemaDetailCache.invalidateAll();

        // Reload schema cache by scanning all schemas once
        return delegate.fetchAllSchemas()
                .doOnNext(schemaDefinition -> schemaCache.put(schemaDefinition.reference(), schemaDefinition))
                .then();
    }
}
package com.barclaycardus.webhooksvcs.evntsrc.service;

import com.barclaycardus.webhooksvcs.evntsrc.error.ProcessingException;
import com.barclaycardus.webhooksvcs.evntsrc.error.SchemaValidationException;
import com.barclaycardus.webhooksvcs.evntsrc.error.TransformationException;
import com.barclaycardus.webhooksvcs.evntsrc.model.InboundMessageContext;
import com.barclaycardus.webhooksvcs.evntsrc.model.ProcessedEvent;
import com.barclaycardus.webhooksvcs.evntsrc.publisher.MskKafkaPublisher;
import com.barclaycardus.webhooksvcs.evntsrc.publisher.RetryPublisher;
import com.barclaycardus.webhooksvcs.evntsrc.schema.SchemaMetadata;
import com.barclaycardus.webhooksvcs.evntsrc.schema.SchemaService;
import com.barclaycardus.webhooksvcs.evntsrc.schema.SchemaValidator;
import com.barclaycardus.webhooksvcs.evntsrc.transform.TransformationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

@Service
public class EventProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);

    private final SchemaService schemaService;
    private final SchemaValidator schemaValidator;
    private final TransformationService transformationService;
    private final MskKafkaPublisher publisher;
    private final RetryPublisher retryPublisher;

    public EventProcessingService(SchemaService schemaService,
                                  SchemaValidator schemaValidator,
                                  TransformationService transformationService,
                                  MskKafkaPublisher publisher,
                                  RetryPublisher retryPublisher) {
        this.schemaService = schemaService;
        this.schemaValidator = schemaValidator;
        this.transformationService = transformationService;
        this.publisher = publisher;
        this.retryPublisher = retryPublisher;
    }

    public void handle(InboundMessageContext context) {
        String schemaId = context.schemaId()
                .orElseThrow(() -> new ProcessingException("Missing schemaId header"));
        try {
            SchemaMetadata schema = schemaService.resolve(schemaId);
            schemaValidator.validate(schema, context.payload());
            byte[] payload = transformationService.applyTransformations(schema, context.payload());
            String partitionKey = determinePartitionKey(context);
            publisher.publish(new ProcessedEvent(schema, payload, partitionKey));
        } catch (SchemaValidationException validationException) {
            log.warn("Validation error for schema {}", schemaId, validationException);
            retryPublisher.sendToDlq(schemaId, context.payload().getBytes(StandardCharsets.UTF_8), validationException.getMessage());
            throw validationException;
        } catch (TransformationException transformationException) {
            log.error("Transformation error for schema {}", schemaId, transformationException);
            retryPublisher.sendToDlq(schemaId, context.payload().getBytes(StandardCharsets.UTF_8), transformationException.getMessage());
            throw transformationException;
        } catch (Exception exception) {
            log.error("Unexpected processing error for schema {}", schemaId, exception);
            retryPublisher.sendToRetry(schemaId, context.payload().getBytes(StandardCharsets.UTF_8));
            throw new ProcessingException("Unexpected error", exception);
        }
    }

    private String determinePartitionKey(InboundMessageContext context) {
        return context.headers().entrySet().stream()
                .filter(entry -> entry.getKey() != null)
                .filter(entry -> entry.getValue() != null)
                .filter(entry -> {
                    String key = entry.getKey().toLowerCase(Locale.ROOT);
                    return key.contains("eventid") || key.contains("correlationid");
                })
                .map(entry -> String.valueOf(entry.getValue()))
                .findFirst()
                .or(() -> context.fallbackPartitionKey())
                .orElseGet(() -> UUID.randomUUID().toString());
    }
}

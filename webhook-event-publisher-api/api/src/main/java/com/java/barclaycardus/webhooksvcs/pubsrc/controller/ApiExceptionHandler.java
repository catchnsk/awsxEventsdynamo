package com.java.barclaycardus.webhooksvcs.pubsrc.controller;

import com.java.barclaycardus.webhooksvcs.pubsrc.publisher.KafkaPublishException;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.DynamoDbException;
import com.java.barclaycardus.webhooksvcs.pubsrc.validation.SchemaValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@RestControllerAdvice
@Order(-1) // Higher priority than default error handlers
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(SchemaValidationException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleSchemaValidation(SchemaValidationException ex, ServerWebExchange exchange) {
        String path = exchange.getRequest().getPath().value();
        return Mono.just(ResponseEntity.status(BAD_REQUEST)
                .body(new ErrorResponse(
                        Instant.now(),
                        BAD_REQUEST.value(),
                        BAD_REQUEST.getReasonPhrase(),
                        ex.getMessage(),
                        path
                )));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleResponseStatusException(ResponseStatusException ex, ServerWebExchange exchange) {
        HttpStatus status = HttpStatus.valueOf(ex.getStatusCode().value());
        String path = exchange.getRequest().getPath().value();
        String message = ex.getReason() != null ? ex.getReason() : ex.getMessage();
        
        log.debug("Handling ResponseStatusException: status={}, message={}, path={}", status, message, path);
        
        return Mono.just(ResponseEntity.status(status)
                .body(new ErrorResponse(
                        Instant.now(),
                        status.value(),
                        status.getReasonPhrase(),
                        message,
                        path
                )));
    }

    @ExceptionHandler(DynamoDbException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleDynamoDbException(DynamoDbException ex, ServerWebExchange exchange) {
        HttpStatus status = ex.getMessage().contains("does not exist") 
                ? HttpStatus.INTERNAL_SERVER_ERROR 
                : HttpStatus.SERVICE_UNAVAILABLE;
        String path = exchange.getRequest().getPath().value();
        return Mono.just(ResponseEntity.status(status)
                .body(new ErrorResponse(
                        Instant.now(),
                        status.value(),
                        status.getReasonPhrase(),
                        ex.getMessage(),
                        path
                )));
    }

    @ExceptionHandler(KafkaPublishException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleKafkaPublishException(KafkaPublishException ex, ServerWebExchange exchange) {
        String path = exchange.getRequest().getPath().value();
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(
                        Instant.now(),
                        HttpStatus.SERVICE_UNAVAILABLE.value(),
                        HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                        ex.getMessage(),
                        path
                )));
    }

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<ErrorResponse>> handleGenericException(Exception ex, ServerWebExchange exchange) {
        String path = exchange.getRequest().getPath().value();
        String message = ex.getMessage();
        String exceptionType = ex.getClass().getName();
        
        log.error("Handling unhandled exception: type={}, message={}, path={}", exceptionType, message, path, ex);
        
        // Check if it's a Kafka-related error
        if (message != null && (message.contains("Kafka") || message.contains("broker") || 
                                message.contains("connection") || message.contains("timeout") ||
                                ex.getCause() instanceof KafkaPublishException)) {
            return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new ErrorResponse(
                            Instant.now(),
                            HttpStatus.SERVICE_UNAVAILABLE.value(),
                            HttpStatus.SERVICE_UNAVAILABLE.getReasonPhrase(),
                            "Kafka is unavailable: " + message,
                            path
                    )));
        }
        
        // For other unhandled exceptions, return 500 with message
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(
                        Instant.now(),
                        HttpStatus.INTERNAL_SERVER_ERROR.value(),
                        HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                        message != null ? message : "An unexpected error occurred: " + exceptionType,
                        path
                )));
    }

    public record ErrorResponse(
            Instant timestamp,
            int status,
            String error,
            String message,
            String path
    ) {}
}

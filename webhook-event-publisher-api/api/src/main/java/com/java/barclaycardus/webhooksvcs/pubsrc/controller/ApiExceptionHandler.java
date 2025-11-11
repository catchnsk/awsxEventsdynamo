package com.java.barclaycardus.webhooksvcs.pubsrc.controller;

import com.java.barclaycardus.webhooksvcs.pubsrc.validation.SchemaValidationException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(SchemaValidationException.class)
    public ResponseEntity<Map<String, String>> handleSchemaValidation(SchemaValidationException ex) {
        return ResponseEntity.status(BAD_REQUEST)
                .body(Map.of("error", ex.getMessage()));
    }
}

package com.java.barclaycardus.webhooksvcs.pubsrc.schema;

/**
 * Exception thrown when DynamoDB operations fail.
 * This exception indicates that the schema service could not access DynamoDB,
 * typically due to service unavailability, table not found, or network issues.
 */
public class DynamoDbException extends RuntimeException {
    
    public DynamoDbException(String message) {
        super(message);
    }
    
    public DynamoDbException(String message, Throwable cause) {
        super(message, cause);
    }
}


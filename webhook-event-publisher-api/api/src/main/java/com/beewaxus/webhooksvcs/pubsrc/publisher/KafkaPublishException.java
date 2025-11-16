package com.beewaxus.webhooksvcs.pubsrc.publisher;

/**
 * Exception thrown when Kafka publish operations fail.
 * This exception indicates that the event could not be published to Kafka,
 * typically due to broker unavailability, network issues, or timeout.
 */
public class KafkaPublishException extends RuntimeException {
    
    public KafkaPublishException(String message) {
        super(message);
    }
    
    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}


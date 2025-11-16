package com.java.barclaycardus.webhooksvcs.pubsrc.publisher;

import com.java.barclaycardus.webhooksvcs.pubsrc.model.EventEnvelope;
import reactor.core.publisher.Mono;

public interface EventPublisher {

    Mono<String> publish(EventEnvelope envelope);
    
    Mono<String> publishAvro(EventEnvelope envelope, String topicName, byte[] avroBytes);
}

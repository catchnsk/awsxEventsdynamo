package com.beewaxus.webhooksvcs.pubsrc.publisher;

import com.beewaxus.webhooksvcs.pubsrc.model.EventEnvelope;
import reactor.core.publisher.Mono;

public interface EventPublisher {

    Mono<String> publish(EventEnvelope envelope);
    
    Mono<String> publishAvro(EventEnvelope envelope, String topicName, byte[] avroBytes);
}

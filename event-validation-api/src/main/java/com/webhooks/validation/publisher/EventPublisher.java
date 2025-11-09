package com.webhooks.validation.publisher;

import com.webhooks.validation.model.EventEnvelope;
import reactor.core.publisher.Mono;

public interface EventPublisher {

    Mono<String> publish(EventEnvelope envelope);
}

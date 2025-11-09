package com.webhooks.sdk.core;

import java.time.Duration;
import java.time.Instant;

public record PublishResponse(String eventId, boolean accepted, Instant acceptedAt, Duration latency) {
    public static PublishResponse accepted(String eventId, Instant acceptedAt, Duration latency) {
        return new PublishResponse(eventId, true, acceptedAt, latency);
    }
}

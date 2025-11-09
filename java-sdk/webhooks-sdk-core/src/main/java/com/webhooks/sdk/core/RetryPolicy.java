package com.webhooks.sdk.core;

import java.time.Duration;

public final class RetryPolicy {

    private final int maxAttempts;
    private final Duration baseDelay;

    public RetryPolicy(int maxAttempts, Duration baseDelay) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.baseDelay = baseDelay;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration baseDelay() {
        return baseDelay;
    }

    public Duration computeDelay(int attempt) {
        long multiplier = (long) Math.pow(2, attempt - 1);
        return baseDelay.multipliedBy(multiplier);
    }
}

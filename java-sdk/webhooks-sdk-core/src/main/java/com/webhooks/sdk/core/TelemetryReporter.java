package com.webhooks.sdk.core;

public interface TelemetryReporter {

    void onSuccess(Event event, PublishResponse response);

    void onFailure(Event event, Exception exception, int attempt);

    TelemetryReporter NOOP = new TelemetryReporter() {
        @Override
        public void onSuccess(Event event, PublishResponse response) {}

        @Override
        public void onFailure(Event event, Exception exception, int attempt) {}
    };
}

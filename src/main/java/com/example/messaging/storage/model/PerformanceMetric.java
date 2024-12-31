package com.example.messaging.storage.model;

import java.time.Instant;

public class PerformanceMetric {
    private final long value;
    private final Instant timestamp;

    public PerformanceMetric(long value) {
        this.value = value;
        this.timestamp = Instant.now();
    }

    public long getValue() {
        return value;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
}

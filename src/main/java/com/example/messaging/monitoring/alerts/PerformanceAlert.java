package com.example.messaging.monitoring.alerts;

import java.time.Instant;

public class PerformanceAlert {
    private final String title;
    private final String description;
    private final Instant timestamp;
    private final AlertSeverity severity;

    public enum AlertSeverity {
        INFO,
        WARNING,
        CRITICAL
    }

    public PerformanceAlert(String title, String description, AlertSeverity severity) {
        this.title = title;
        this.description = description;
        this.timestamp = Instant.now();
        this.severity = severity;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public AlertSeverity getSeverity() {
        return severity;
    }
}

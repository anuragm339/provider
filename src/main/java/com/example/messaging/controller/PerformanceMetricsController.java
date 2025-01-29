package com.example.messaging.controller;

import com.example.messaging.monitoring.metrics.ConsumerPerformanceTracker;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.Map;

@Controller("/api/metrics")
public class PerformanceMetricsController {

    @Inject
    private ConsumerPerformanceTracker performanceTracker;

    @Serdeable // Add this annotation to the response class
    public static class PerformanceMetricsResponse {
        private final ConsumerPerformanceTracker.GlobalPerformanceSnapshot global;
        private final Map<String, ConsumerPerformanceTracker.ConsumerPerformanceSnapshot> consumers;

        public PerformanceMetricsResponse(
                ConsumerPerformanceTracker.GlobalPerformanceSnapshot global,
                Map<String, ConsumerPerformanceTracker.ConsumerPerformanceSnapshot> consumers) {
            this.global = global;
            this.consumers = consumers;
        }

        public ConsumerPerformanceTracker.GlobalPerformanceSnapshot getGlobal() { return global; }
        public Map<String, ConsumerPerformanceTracker.ConsumerPerformanceSnapshot> getConsumers() { return consumers; }
    }

    @Get("/performance")
    @Produces(MediaType.APPLICATION_JSON)
    public PerformanceMetricsResponse getPerformanceMetrics() {
        return new PerformanceMetricsResponse(
                performanceTracker.getGlobalSnapshot(),
                performanceTracker.getConsumerSnapshots()
        );
    }

    @Get("/performance/{consumerId}")
    @Produces(MediaType.APPLICATION_JSON)
    public ConsumerPerformanceTracker.ConsumerPerformanceSnapshot getConsumerMetrics(String consumerId) {
        return performanceTracker.getConsumerSnapshots().get(consumerId);
    }
}

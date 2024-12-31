package com.example.messaging.monitoring.metrics;

import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.storage.model.PerformanceMetric;
import com.example.messaging.monitoring.alerts.PerformanceAlert;
import com.example.messaging.storage.model.TrendAnalysis;
import com.example.messaging.storage.model.MessageStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MessageStatistics {
    private static final Logger logger = LoggerFactory.getLogger(MessageStatistics.class);

    private final DataSource dataSource;
    private final Map<String, AtomicLong> writeTimings = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> readTimings = new ConcurrentHashMap<>();
    private final Map<String, List<PerformanceMetric>> hourlyMetrics = new ConcurrentHashMap<>();
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);

    private static final long MAX_AVG_WRITE_TIME_MS = 100;
    private static final long MAX_AVG_READ_TIME_MS = 50;
    private static final int METRICS_RETENTION_HOURS = 24;

    private static final String COUNT_QUERY =
            "SELECT COUNT(*), AVG(LENGTH(data)) FROM messages";
    private static final String TYPE_COUNT_QUERY =
            "SELECT type, COUNT(*) FROM messages GROUP BY type";
    private static final String PERFORMANCE_QUERY =
            "SELECT AVG(processing_time) FROM processing_results WHERE created_utc > ?";

    public MessageStatistics(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void recordMetric(String metricType, long value) {
        List<PerformanceMetric> metrics = hourlyMetrics.computeIfAbsent(
                metricType,
                k -> Collections.synchronizedList(new ArrayList<>())
        );

        metrics.add(new PerformanceMetric(value));
        cleanOldMetrics(metrics);
    }

    public void recordWriteTime(String type, long timeMs) {
        writeTimings.computeIfAbsent(type, k -> new AtomicLong())
                .addAndGet(timeMs);
        totalMessagesProcessed.incrementAndGet();
        recordMetric("write", timeMs);
    }

    public void recordReadTime(String type, long timeMs) {
        readTimings.computeIfAbsent(type, k -> new AtomicLong())
                .addAndGet(timeMs);
        recordMetric("read", timeMs);
    }

    private void cleanOldMetrics(List<PerformanceMetric> metrics) {
        Instant cutoff = Instant.now().minusSeconds(METRICS_RETENTION_HOURS * 3600);
        metrics.removeIf(metric -> metric.getTimestamp().isBefore(cutoff));
    }

    public List<PerformanceAlert> checkPerformance() {
        List<PerformanceAlert> alerts = new ArrayList<>();

        double avgWriteTime = calculateAverageWriteTime();
        if (avgWriteTime > MAX_AVG_WRITE_TIME_MS) {
            alerts.add(new PerformanceAlert(
                    "High write latency",
                    String.format("Average write time (%.2fms) exceeds threshold (%dms)",
                            avgWriteTime, MAX_AVG_WRITE_TIME_MS),
                    PerformanceAlert.AlertSeverity.WARNING
            ));
        }

        double avgReadTime = calculateAverageReadTime();
        if (avgReadTime > MAX_AVG_READ_TIME_MS) {
            alerts.add(new PerformanceAlert(
                    "High read latency",
                    String.format("Average read time (%.2fms) exceeds threshold (%dms)",
                            avgReadTime, MAX_AVG_READ_TIME_MS),
                    PerformanceAlert.AlertSeverity.WARNING
            ));
        }

        return alerts;
    }

    public Map<String, TrendAnalysis> analyzeTrends() {
        Map<String, TrendAnalysis> trends = new HashMap<>();

        for (Map.Entry<String, List<PerformanceMetric>> entry : hourlyMetrics.entrySet()) {
            String metricType = entry.getKey();
            List<PerformanceMetric> metrics = entry.getValue();

            if (metrics.size() >= 2) {
                double currentAvg = calculateRecentAverage(metrics, 1);
                double previousAvg = calculateRecentAverage(metrics, 2);
                double changePercent = ((currentAvg - previousAvg) / previousAvg) * 100;

                trends.put(metricType, new TrendAnalysis(
                        currentAvg,
                        previousAvg,
                        changePercent
                ));
            }
        }

        return trends;
    }

    private double calculateRecentAverage(List<PerformanceMetric> metrics, int hoursPrior) {
        Instant start = Instant.now().minusSeconds(hoursPrior * 3600);
        Instant end = start.plusSeconds(3600);

        return metrics.stream()
                .filter(m -> !m.getTimestamp().isBefore(start) && !m.getTimestamp().isAfter(end))
                .mapToLong(m -> m.getValue())
                .average()
                .orElse(0.0);
    }

    private double calculateAverageWriteTime() {
        return calculateAverageTime(writeTimings);
    }

    private double calculateAverageReadTime() {
        return calculateAverageTime(readTimings);
    }

    private double calculateAverageTime(Map<String, AtomicLong> timings) {
        if (timings.isEmpty()) {
            return 0.0;
        }

        long total = timings.values().stream()
                .mapToLong(AtomicLong::get)
                .sum();

        return (double) total / totalMessagesProcessed.get();
    }

    public MessageStats getStatistics() {
        try (Connection conn = dataSource.getConnection()) {
            long totalMessages = 0;
            double avgMessageSize = 0;
            Map<String, Long> typeCount = new HashMap<>();

            // Get basic counts and averages
            try (PreparedStatement stmt = conn.prepareStatement(COUNT_QUERY)) {
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    totalMessages = rs.getLong(1);
                    avgMessageSize = rs.getDouble(2);
                }
            }

            // Get message counts by type
            try (PreparedStatement stmt = conn.prepareStatement(TYPE_COUNT_QUERY)) {
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    typeCount.put(rs.getString(1), rs.getLong(2));
                }
            }

            // Get recent performance metrics
            double avgProcessingTime = 0;
            Instant hourAgo = Instant.now().minusSeconds(3600);
            try (PreparedStatement stmt = conn.prepareStatement(PERFORMANCE_QUERY)) {
                stmt.setTimestamp(1, java.sql.Timestamp.from(hourAgo));
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    avgProcessingTime = rs.getDouble(1);
                }
            }

            return MessageStats.builder()
                    .totalMessages(totalMessages)
                    .averageMessageSize(avgMessageSize)
                    .messageCountByType(typeCount)
                    .averageProcessingTime(avgProcessingTime)
                    .averageWriteTime(calculateAverageWriteTime())
                    .averageReadTime(calculateAverageReadTime())
                    .totalProcessed(totalMessagesProcessed.get())
                    .build();

        } catch (SQLException e) {
            logger.error("Failed to collect message statistics", e);
            throw new ProcessingException(
                    "Failed to collect statistics",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public void reset() {
        writeTimings.clear();
        readTimings.clear();
        hourlyMetrics.clear();
        totalMessagesProcessed.set(0);
    }
}

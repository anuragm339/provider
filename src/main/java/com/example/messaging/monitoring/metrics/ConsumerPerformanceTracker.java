package com.example.messaging.monitoring.metrics;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class ConsumerPerformanceTracker {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerPerformanceTracker.class);

    // Per-consumer metrics
    private final Map<String, ConsumerMetrics> consumerMetrics = new ConcurrentHashMap<>();

    // Global metrics
    private final AtomicLong totalMessagesSent = new AtomicLong(0);
    private final AtomicLong totalBatchesSent = new AtomicLong(0);
    private final AtomicLong totalDataSent = new AtomicLong(0);
    private final AtomicLong totalAckTime = new AtomicLong(0);

    @Serdeable
    public static class ConsumerMetrics {
        private final String consumerId;
        private final AtomicLong messagesSent = new AtomicLong(0);
        private final AtomicLong batchesSent = new AtomicLong(0);
        private final AtomicLong dataSentBytes = new AtomicLong(0);
        private final AtomicLong totalAckTimeMs = new AtomicLong(0);
        private final AtomicLong failedDeliveries = new AtomicLong(0);
        private Instant lastMessageTime;
        private final Queue<Long> recentLatencies = new LinkedList<>();
        private static final int MAX_LATENCY_SAMPLES = 1000;

        public ConsumerMetrics(String consumerId) {
            this.consumerId = consumerId;
        }

        public synchronized void recordMessageSent(long messageSize, long ackTimeMs) {
            messagesSent.incrementAndGet();
            dataSentBytes.addAndGet(messageSize);
            totalAckTimeMs.addAndGet(ackTimeMs);
            lastMessageTime = Instant.now();

            // Record latency
            recentLatencies.offer(ackTimeMs);
            if (recentLatencies.size() > MAX_LATENCY_SAMPLES) {
                recentLatencies.poll();
            }
        }

        public synchronized void recordBatchSent(int batchSize, long totalSize, long ackTimeMs) {
            batchesSent.incrementAndGet();
            messagesSent.addAndGet(batchSize);
            dataSentBytes.addAndGet(totalSize);
            totalAckTimeMs.addAndGet(ackTimeMs);
            lastMessageTime = Instant.now();
        }

        public synchronized void recordFailedDelivery() {
            failedDeliveries.incrementAndGet();
        }

        public ConsumerPerformanceSnapshot getSnapshot() {
            double avgLatency = recentLatencies.isEmpty() ? 0 :
                    recentLatencies.stream().mapToLong(l -> l).average().orElse(0);

            return new ConsumerPerformanceSnapshot(
                    consumerId,
                    messagesSent.get(),
                    batchesSent.get(),
                    dataSentBytes.get(),
                    totalAckTimeMs.get(),
                    failedDeliveries.get(),
                    lastMessageTime,
                    avgLatency
            );
        }
    }

    @Serdeable
    public static class ConsumerPerformanceSnapshot {
        private final String consumerId;
        private final long messagesSent;
        private final long batchesSent;
        private final long dataSentBytes;
        private final long totalAckTimeMs;
        private final long failedDeliveries;
        private final Instant lastMessageTime;
        private final double averageLatencyMs;

        public ConsumerPerformanceSnapshot(
                String consumerId, long messagesSent, long batchesSent,
                long dataSentBytes, long totalAckTimeMs, long failedDeliveries,
                Instant lastMessageTime, double averageLatencyMs) {
            this.consumerId = consumerId;
            this.messagesSent = messagesSent;
            this.batchesSent = batchesSent;
            this.dataSentBytes = dataSentBytes;
            this.totalAckTimeMs = totalAckTimeMs;
            this.failedDeliveries = failedDeliveries;
            this.lastMessageTime = lastMessageTime;
            this.averageLatencyMs = averageLatencyMs;
        }

        // Getters
        public String getConsumerId() { return consumerId; }
        public long getMessagesSent() { return messagesSent; }
        public long getBatchesSent() { return batchesSent; }
        public long getDataSentBytes() { return dataSentBytes; }
        public double getThroughputBytesPerSecond() {
            if (totalAckTimeMs == 0) return 0;
            return (dataSentBytes * 1000.0) / totalAckTimeMs;
        }
        public double getMessagesPerSecond() {
            if (totalAckTimeMs == 0) return 0;
            return (messagesSent * 1000.0) / totalAckTimeMs;
        }
        public long getFailedDeliveries() { return failedDeliveries; }
        public Instant getLastMessageTime() { return lastMessageTime; }
        public double getAverageLatencyMs() { return averageLatencyMs; }
    }

    // Methods to track performance
    public void recordMessageSent(String consumerId, long messageSize, long ackTimeMs) {
        ConsumerMetrics metrics = consumerMetrics.computeIfAbsent(
                consumerId,
                ConsumerMetrics::new
        );
        metrics.recordMessageSent(messageSize, ackTimeMs);

        // Update global metrics
        totalMessagesSent.incrementAndGet();
        totalDataSent.addAndGet(messageSize);
        totalAckTime.addAndGet(ackTimeMs);
    }

    public void recordBatchSent(String consumerId, int batchSize, long totalSize, long ackTimeMs) {
        ConsumerMetrics metrics = consumerMetrics.computeIfAbsent(
                consumerId,
                ConsumerMetrics::new
        );
        metrics.recordBatchSent(batchSize, totalSize, ackTimeMs);

        // Update global metrics
        totalBatchesSent.incrementAndGet();
        totalMessagesSent.addAndGet(batchSize);
        totalDataSent.addAndGet(totalSize);
        totalAckTime.addAndGet(ackTimeMs);
    }

    public void recordFailedDelivery(String consumerId) {
        ConsumerMetrics metrics = consumerMetrics.computeIfAbsent(
                consumerId,
                ConsumerMetrics::new
        );
        metrics.recordFailedDelivery();
    }

    // Methods to get performance metrics
    public Map<String, ConsumerPerformanceSnapshot> getConsumerSnapshots() {
        Map<String, ConsumerPerformanceSnapshot> snapshots = new HashMap<>();
        consumerMetrics.forEach((consumerId, metrics) ->
                snapshots.put(consumerId, metrics.getSnapshot())
        );
        return snapshots;
    }

    public GlobalPerformanceSnapshot getGlobalSnapshot() {
        return new GlobalPerformanceSnapshot(
                totalMessagesSent.get(),
                totalBatchesSent.get(),
                totalDataSent.get(),
                totalAckTime.get()
        );
    }
    @Serdeable
    public static class GlobalPerformanceSnapshot {
        private final long totalMessagesSent;
        private final long totalBatchesSent;
        private final long totalDataSent;
        private final long totalAckTime;

        public GlobalPerformanceSnapshot(
                long totalMessagesSent,
                long totalBatchesSent,
                long totalDataSent,
                long totalAckTime) {
            this.totalMessagesSent = totalMessagesSent;
            this.totalBatchesSent = totalBatchesSent;
            this.totalDataSent = totalDataSent;
            this.totalAckTime = totalAckTime;
        }

        public long getTotalMessagesSent() { return totalMessagesSent; }
        public long getTotalBatchesSent() { return totalBatchesSent; }
        public long getTotalDataSent() { return totalDataSent; }
        public double getGlobalThroughputBytesPerSecond() {
            if (totalAckTime == 0) return 0;
            return (totalDataSent * 1000.0) / totalAckTime;
        }
        public double getGlobalMessagesPerSecond() {
            if (totalAckTime == 0) return 0;
            return (totalMessagesSent * 1000.0) / totalAckTime;
        }
    }
}

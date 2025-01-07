package com.example.messaging.core.pipeline.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.inject.Singleton;

@ConfigurationProperties("pipeline.queue")
@Singleton
public class QueueConfiguration {
    private long maxQueueSizeBytes = 1024 * 1024; // Default 1MB
    private int initialQueueCapacity = 1000;
    private boolean enforceByteLimit = true;
    private int maxMessagesPerConsumer = 10000;

    // Memory management
    private double memoryThresholdPercentage = 0.8; // 80% threshold
    private long minimumFreeMemoryBytes = 100 * 1024 * 1024; // 100MB minimum free

    public long getMaxQueueSizeBytes() {
        return maxQueueSizeBytes;
    }

    public void setMaxQueueSizeBytes(long maxQueueSizeBytes) {
        this.maxQueueSizeBytes = maxQueueSizeBytes;
    }

    public int getInitialQueueCapacity() {
        return initialQueueCapacity;
    }

    public void setInitialQueueCapacity(int initialQueueCapacity) {
        this.initialQueueCapacity = initialQueueCapacity;
    }

    public boolean isEnforceByteLimit() {
        return enforceByteLimit;
    }

    public void setEnforceByteLimit(boolean enforceByteLimit) {
        this.enforceByteLimit = enforceByteLimit;
    }

    public int getMaxMessagesPerConsumer() {
        return maxMessagesPerConsumer;
    }

    public void setMaxMessagesPerConsumer(int maxMessagesPerConsumer) {
        this.maxMessagesPerConsumer = maxMessagesPerConsumer;
    }

    public double getMemoryThresholdPercentage() {
        return memoryThresholdPercentage;
    }

    public void setMemoryThresholdPercentage(double memoryThresholdPercentage) {
        this.memoryThresholdPercentage = memoryThresholdPercentage;
    }

    public long getMinimumFreeMemoryBytes() {
        return minimumFreeMemoryBytes;
    }

    public void setMinimumFreeMemoryBytes(long minimumFreeMemoryBytes) {
        this.minimumFreeMemoryBytes = minimumFreeMemoryBytes;
    }
}

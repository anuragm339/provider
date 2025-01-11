package com.example.messaging.core.pipeline.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;

@ConfigurationProperties("pipeline.queue")
@Singleton
public class QueueConfiguration {
    @Value("${pipeline.queue.maxQueueSizeBytes:1048576}")
    private long maxQueueSizeBytes;
    @Value("${pipeline.queue.initialQueueCapacity:1000}")
    private int initialQueueCapacity;
    @Value("${pipeline.queue.enforceByteLimit:true}")
    private boolean enforceByteLimit;
    @Value("${pipeline.queue.maxMessagesPerConsumer:10000}")
    private int maxMessagesPerConsumer;

    // Memory management
    @Value("${pipeline.queue.memoryThresholdPercentage:0.8}")
    private double memoryThresholdPercentage; // 80% threshold
    @Value("${pipeline.queue.minimumFreeMemoryBytes:104857600}")
    private long minimumFreeMemoryBytes; // 100MB minimum free

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

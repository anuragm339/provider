package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.ParallelProcessingConfig;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Primary;
import jakarta.inject.Singleton;

@Singleton
@Primary
public class ProviderParallelProcessingConfig implements ParallelProcessingConfig {
    private int parallelThreads = Runtime.getRuntime().availableProcessors();
    private int queueSizePerThread = 1000;
    private int maxBatchSize = 1000;
    private int minBatchSize = 100;
    private boolean maintainOrderWithinBatch = true;
    private int batchTimeoutMs = 30000;
    private int maxBatchRetries = 3;

    @Override
    public int getParallelThreads() {
        return parallelThreads;
    }

    public void setParallelThreads(int parallelThreads) {
        this.parallelThreads = parallelThreads;
    }

    @Override
    public int getQueueSizePerThread() {
        return queueSizePerThread;
    }

    public void setQueueSizePerThread(int queueSizePerThread) {
        this.queueSizePerThread = queueSizePerThread;
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public int getMinBatchSize() {
        return minBatchSize;
    }

    public void setMinBatchSize(int minBatchSize) {
        this.minBatchSize = minBatchSize;
    }

    @Override
    public boolean maintainOrderWithinBatch() {
        return maintainOrderWithinBatch;
    }

    public void setMaintainOrderWithinBatch(boolean maintainOrderWithinBatch) {
        this.maintainOrderWithinBatch = maintainOrderWithinBatch;
    }

    @Override
    public int getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    public void setBatchTimeoutMs(int batchTimeoutMs) {
        this.batchTimeoutMs = batchTimeoutMs;
    }

    @Override
    public int getMaxBatchRetries() {
        return maxBatchRetries;
    }

    public void setMaxBatchRetries(int maxBatchRetries) {
        this.maxBatchRetries = maxBatchRetries;
    }
}

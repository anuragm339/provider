package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.ParallelProcessingConfig;
import io.micronaut.context.annotation.Primary;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public class DefaultParallelProcessingConfig implements ParallelProcessingConfig {

    private final int parallelThreads;
    private final int queueSizePerThread;
    private final int maxBatchSize;
    private final int minBatchSize;
    private final boolean maintainOrderWithinBatch;
    private final int batchTimeoutMs;
    private final int maxBatchRetries;

    public DefaultParallelProcessingConfig(Builder builder) {
        this.parallelThreads = builder.parallelThreads;
        this.queueSizePerThread = builder.queueSizePerThread;
        this.maxBatchSize = builder.maxBatchSize;
        this.minBatchSize = builder.minBatchSize;
        this.maintainOrderWithinBatch = builder.maintainOrderWithinBatch;
        this.batchTimeoutMs = builder.batchTimeoutMs;
        this.maxBatchRetries = builder.maxBatchRetries;
    }

    @Override
    public int getParallelThreads() {
        return parallelThreads;
    }

    @Override
    public int getQueueSizePerThread() {
        return queueSizePerThread;
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public int getMinBatchSize() {
        return minBatchSize;
    }

    @Override
    public boolean maintainOrderWithinBatch() {
        return maintainOrderWithinBatch;
    }

    @Override
    public int getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    @Override
    public int getMaxBatchRetries() {
        return maxBatchRetries;
    }

    public static class Builder {
        private int parallelThreads = Runtime.getRuntime().availableProcessors();
        private int queueSizePerThread = 1000;
        private int maxBatchSize = 1000;
        private int minBatchSize = 100;
        private boolean maintainOrderWithinBatch = true;
        private int batchTimeoutMs = 30000; // 30 seconds default
        private int maxBatchRetries = 3;    // 3 retries default

        public Builder parallelThreads(int parallelThreads) {
            this.parallelThreads = parallelThreads;
            return this;
        }

        public Builder queueSizePerThread(int queueSizePerThread) {
            this.queueSizePerThread = queueSizePerThread;
            return this;
        }

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder minBatchSize(int minBatchSize) {
            this.minBatchSize = minBatchSize;
            return this;
        }

        public Builder maintainOrderWithinBatch(boolean maintainOrderWithinBatch) {
            this.maintainOrderWithinBatch = maintainOrderWithinBatch;
            return this;
        }

        public Builder batchTimeoutMs(int batchTimeoutMs) {
            this.batchTimeoutMs = batchTimeoutMs;
            return this;
        }

        public Builder maxBatchRetries(int maxBatchRetries) {
            this.maxBatchRetries = maxBatchRetries;
            return this;
        }

        public DefaultParallelProcessingConfig build() {
            validate();
            return new DefaultParallelProcessingConfig(this);
        }

        private void validate() {
            if (parallelThreads <= 0) {
                throw new IllegalArgumentException("Parallel threads must be greater than 0");
            }
            if (queueSizePerThread <= 0) {
                throw new IllegalArgumentException("Queue size per thread must be greater than 0");
            }
            if (maxBatchSize < minBatchSize) {
                throw new IllegalArgumentException("Max batch size must be greater than or equal to min batch size");
            }
            if (minBatchSize <= 0) {
                throw new IllegalArgumentException("Min batch size must be greater than 0");
            }
            if (batchTimeoutMs <= 0) {
                throw new IllegalArgumentException("Batch timeout must be greater than 0");
            }
            if (maxBatchRetries < 0) {
                throw new IllegalArgumentException("Max batch retries cannot be negative");
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}

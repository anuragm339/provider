package com.example.messaging.core.pipeline.service;

public interface ParallelProcessingConfig {
    /**
     * Get number of parallel processing threads
     */
    int getParallelThreads();

    /**
     * Get queue size per thread
     */
    int getQueueSizePerThread();

    /**
     * Get maximum batch size for parallel processing
     */
    int getMaxBatchSize();

    /**
     * Get minimum batch size for parallel processing
     */
    int getMinBatchSize();

    /**
     * Check if order needs to be maintained within a batch
     */
    boolean maintainOrderWithinBatch();

    /**
     * Get timeout for batch processing in milliseconds
     */
    int getBatchTimeoutMs();

    /**
     * Get maximum number of retries for a failed batch
     */
    int getMaxBatchRetries();
}

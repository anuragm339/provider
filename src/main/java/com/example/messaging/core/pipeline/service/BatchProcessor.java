package com.example.messaging.core.pipeline.service;

import com.example.messaging.models.Message;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface BatchProcessor {
    /**
     * Process a batch of messages
     * @param messages List of messages to process
     * @return Future containing list of processing results
     */
    CompletableFuture<List<ProcessingResult>> processBatch(List<Message> messages);

    /**
     * Check if batch can be accepted
     * @param batchSize Size of the batch
     * @return true if batch can be processed
     */
    boolean canAcceptBatch(int batchSize);

    /**
     * Get optimal batch size based on current system state
     * @return optimal batch size
     */
    int getOptimalBatchSize();

    /**
     * Verify batch processing
     * @param batchId Unique batch identifier
     * @return true if batch was processed successfully
     */
    boolean verifyBatchProcessing(String batchId);
}

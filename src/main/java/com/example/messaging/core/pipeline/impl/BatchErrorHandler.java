package com.example.messaging.core.pipeline.impl;

import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.core.pipeline.model.MessageBatch;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
@Singleton
public class BatchErrorHandler {
    private static final Logger logger = LoggerFactory.getLogger(BatchErrorHandler.class);

    private final Map<String, BatchErrorState> errorStates = new ConcurrentHashMap<>();
    private final int maxConsecutiveFailures;
    private final long batchExpiryMs;

    public BatchErrorHandler(int maxConsecutiveFailures, long batchExpiryMs) {
        this.maxConsecutiveFailures = maxConsecutiveFailures;
        this.batchExpiryMs = batchExpiryMs;
    }

    public void validateBatchResources(MessageBatch batch, int availableThreads, long freeMemory) {
        // Check thread pool capacity
        if (availableThreads < batch.getBatchSize()) {
            throw new ProcessingException(
                    "Insufficient thread pool capacity",
                    ErrorCode.THREAD_POOL_EXHAUSTED.getCode(),
                    true,
                    null
            );
        }

        // Check memory threshold (assuming 1MB per message as example)
        long requiredMemory = batch.getBatchSize() * 1024 * 1024;
        if (freeMemory < requiredMemory) {
            throw new ProcessingException(
                    "Insufficient memory for batch processing",
                    ErrorCode.MEMORY_EXHAUSTED.getCode(),
                    true,
                    null
            );
        }
    }

    public void validateBatchState(MessageBatch batch) {
        // Check if batch is expired
        if (isBatchExpired(batch)) {
            throw new ProcessingException(
                    "Batch has expired: " + batch.getBatchId(),
                    ErrorCode.BATCH_EXPIRED.getCode(),
                    false,
                    null
            );
        }

        // Check for duplicate processing
        BatchErrorState errorState = errorStates.get(batch.getBatchId());
        if (errorState != null && errorState.isProcessing()) {
            throw new ProcessingException(
                    "Batch is already being processed: " + batch.getBatchId(),
                    ErrorCode.BATCH_STATE_INVALID.getCode(),
                    false,
                    null
            );
        }
    }

    public void validateBatchSequence(MessageBatch batch) {
        // Check for sequence gaps
        long expectedOffset = batch.getStartOffset();
        for (int i = 0; i < batch.getBatchSize(); i++) {
            long actualOffset = batch.getMessages().get(i).getMsgOffset();
            if (actualOffset != expectedOffset + i) {
                throw new ProcessingException(
                        "Sequence gap detected in batch",
                        ErrorCode.INVALID_MESSAGE_SEQUENCE.getCode(),
                        false,
                        null
                );
            }
        }

        // Check for duplicates
        if (hasDuplicateMessages(batch.getMessages())) {
            throw new ProcessingException(
                    "Duplicate messages detected in batch",
                    ErrorCode.DUPLICATE_MESSAGE.getCode(),
                    false,
                    null
            );
        }
    }

    public void handleBatchError(String batchId, Exception e) {
        BatchErrorState errorState = errorStates.computeIfAbsent(
                batchId,
                k -> new BatchErrorState()
        );

        errorState.incrementFailures();

        if (errorState.getConsecutiveFailures() >= maxConsecutiveFailures) {
            logger.error("Batch {} exceeded maximum consecutive failures", batchId);
            throw new ProcessingException(
                    "Maximum consecutive failures exceeded for batch",
                    ErrorCode.RETRY_LIMIT_EXCEEDED.getCode(),
                    false,
                    e
            );
        }
    }

    public void handlePartialBatchFailure(String batchId, int failedCount, int totalCount) {
        if (failedCount > 0) {
            throw new ProcessingException(
                    String.format("Partial batch failure: %d/%d messages failed", failedCount, totalCount),
                    ErrorCode.PARTIAL_BATCH_FAILURE.getCode(),
                    true,
                    null
            );
        }
    }

    public void clearErrorState(String batchId) {
        errorStates.remove(batchId);
    }

    private boolean isBatchExpired(MessageBatch batch) {
        return System.currentTimeMillis() - batch.getCreatedAt().toEpochMilli() > batchExpiryMs;
    }

    private boolean hasDuplicateMessages(List<?> messages) {
        return messages.stream().distinct().count() != messages.size();
    }

    private static class BatchErrorState {
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        private volatile boolean processing = false;

        public void incrementFailures() {
            consecutiveFailures.incrementAndGet();
        }

        public int getConsecutiveFailures() {
            return consecutiveFailures.get();
        }

        public boolean isProcessing() {
            return processing;
        }

        public void setProcessing(boolean processing) {
            this.processing = processing;
        }
    }
}

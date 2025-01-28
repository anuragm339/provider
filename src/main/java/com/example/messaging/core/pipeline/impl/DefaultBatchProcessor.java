package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.model.BatchStatus;
import com.example.messaging.core.pipeline.service.BatchProcessor;
import com.example.messaging.core.pipeline.service.MessageProcessor;
import com.example.messaging.core.pipeline.service.ParallelProcessingConfig;
import com.example.messaging.core.pipeline.service.ProcessingResult;
import com.example.messaging.core.pipeline.model.MessageBatch;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.models.Message;

import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Singleton
public class DefaultBatchProcessor implements BatchProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultBatchProcessor.class);

    private final MessageProcessor messageProcessor;
    private final ParallelProcessingConfig config;
    private final ExecutorService executorService;
    private final ConcurrentMap<String, BatchStatus> batchStatuses;
    private final BatchErrorHandler errorHandler;

    public DefaultBatchProcessor(
            MessageProcessor messageProcessor,
            ParallelProcessingConfig config) {
        this.messageProcessor = messageProcessor;
        this.config = config;
        this.executorService = Executors.newFixedThreadPool(config.getParallelThreads());
        this.batchStatuses = new ConcurrentHashMap<>();
        this.errorHandler = new BatchErrorHandler(config.getMaxBatchRetries(), config.getBatchTimeoutMs());
    }

    @Override
    public CompletableFuture<List<ProcessingResult>> processBatch(List<Message> messages) {
        if (!canAcceptBatch(messages.size())) {
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Cannot accept batch of size " + messages.size(),
                            ErrorCode.BATCH_SIZE_INVALID.getCode(),
                            true,
                            null
                    )
            );
        }

        MessageBatch batch = MessageBatch.builder()
                .messages(messages)
                .build();

        try {
            // Validate resources and batch state
            errorHandler.validateBatchResources(
                    batch,
                    config.getParallelThreads(),
                    Runtime.getRuntime().freeMemory()
            );
//            errorHandler.validateBatchState(batch);
            errorHandler.validateBatchSequence(batch);

            batchStatuses.put(batch.getBatchId(), new BatchStatus("","",batch.getBatchSize(),null,0));

            return processBatchWithRetry(batch)
                    .whenComplete((results, error) -> {
                        if (error != null) {
                            errorHandler.handleBatchError(batch.getBatchId(), (Exception) error);
                        } else {
                            errorHandler.clearErrorState(batch.getBatchId());
                        }
                        cleanupBatch(batch.getBatchId());
                    });
        } catch (Exception e) {
            logger.error("Failed to process batch", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<List<ProcessingResult>> processBatchWithRetry(MessageBatch batch) {
        return CompletableFuture.supplyAsync(() -> {
            List<ProcessingResult> results = new ArrayList<>();
            int retryCount = 0;

            while (retryCount <= config.getMaxBatchRetries()) {
                try {
                    results = processMessagesInParallel(batch);
                    validateResults(results, batch);
                    return results;
                } catch (Exception e) {
                    logger.warn("Batch processing attempt {} failed for batch {}",
                            retryCount + 1, batch.getBatchId(), e);
                    retryCount++;
                    if (retryCount <= config.getMaxBatchRetries()) {
                        backoff(retryCount);
                    }
                }
            }

            throw new ProcessingException(
                    "Batch processing failed after " + config.getMaxBatchRetries() + " retries",
                    ErrorCode.RETRY_LIMIT_EXCEEDED.getCode(),
                    false,
                    null
            );
        }, executorService);
    }

    private List<ProcessingResult> processMessagesInParallel(MessageBatch batch) {
        List<CompletableFuture<ProcessingResult>> futures = batch.getMessages().stream()
                .map(this::processMessageWithTimeout)
                .collect(Collectors.toList());

        try {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
            );

            return allFutures
                    .thenApply(v -> {
                        List<ProcessingResult> results = futures.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList());

                        // Check for partial failures
                        long failedCount = results.stream()
                                .filter(r -> !r.isSuccessful())
                                .count();

                        if (failedCount > 0) {
                            errorHandler.handlePartialBatchFailure(
                                    batch.getBatchId(),
                                    (int)failedCount,
                                    batch.getBatchSize()
                            );
                        }

                        return results;
                    })
                    .orTimeout(config.getBatchTimeoutMs(), TimeUnit.MILLISECONDS)
                    .get(config.getBatchTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new ProcessingException(
                    "Failed to process batch within timeout",
                    ErrorCode.TIMEOUT.getCode(),
                    true,
                    e
            );
        }
    }

    private CompletableFuture<ProcessingResult> processMessageWithTimeout(Message message) {
        return messageProcessor.processMessage(message)
                .orTimeout(config.getBatchTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void validateResults(List<ProcessingResult> results, MessageBatch batch) {
        if (results.size() != batch.getBatchSize()) {
            throw new ProcessingException(
                    "Results size mismatch. Expected: " + batch.getBatchSize() + ", Got: " + results.size(),
                    ErrorCode.BATCH_PROCESSING_ERROR.getCode(),
                    false,
                    null
            );
        }

        if (config.maintainOrderWithinBatch()) {
            validateOrder(results, batch);
        }
    }

    private void validateOrder(List<ProcessingResult> results, MessageBatch batch) {
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i).getOffset() != batch.getMessages().get(i).getMsgOffset()) {
                throw new ProcessingException(
                        "Order mismatch in batch processing",
                        ErrorCode.BATCH_SEQUENCE_ERROR.getCode(),
                        false,
                        null
                );
            }
        }
    }

    @Override
    public boolean canAcceptBatch(int batchSize) {
        return messageProcessor.canAccept() &&
                batchSize <= config.getMaxBatchSize() &&
                batchSize >= config.getMinBatchSize();
    }

    @Override
    public int getOptimalBatchSize() {
        return Math.min(
                config.getMaxBatchSize(),
                config.getQueueSizePerThread() * config.getParallelThreads() / 2
        );
    }

    @Override
    public boolean verifyBatchProcessing(String batchId) {
        BatchStatus status = batchStatuses.get(batchId);
        return status != null && status.isComplete();
    }

    private void cleanupBatch(String batchId) {
        batchStatuses.remove(batchId);
    }

    private void backoff(int retryCount) {
        try {
            Thread.sleep(Math.min(1000 * retryCount, 5000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessingException(
                    "Batch processing interrupted during backoff",
                    ErrorCode.BATCH_PROCESSING_ERROR.getCode(),
                    true,
                    e
            );
        }
    }



    // Cleanup method for shutdown
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

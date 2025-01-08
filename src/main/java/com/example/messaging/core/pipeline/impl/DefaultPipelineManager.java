package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.*;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.models.Message;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.core.pipeline.config.QueueConfiguration;
import com.example.messaging.core.pipeline.queue.ByteSizedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DefaultPipelineManager implements PipelineManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPipelineManager.class);

    private final MessageProcessor messageProcessor;
    private final BatchProcessor batchProcessor;
    private final MessageStore messageStore;
    private final ExecutorService primaryExecutor;
    private final ExecutorService completionExecutor;
    private final AtomicReference<PipelineStatus> status;
    private final Map<String, ByteSizedBlockingQueue> typeBasedQueues;
    private final QueueConfiguration queueConfig;
    private final int threadPoolSize;
    private final int maxParallelMessages;

    public DefaultPipelineManager(
            MessageProcessor messageProcessor,
            BatchProcessor batchProcessor,
            MessageStore messageStore,
            ExecutorService executor,
            QueueConfiguration queueConfig,
            int threadPoolSize) {
        this.messageProcessor = messageProcessor;
        this.batchProcessor = batchProcessor;
        this.messageStore = messageStore;
        this.queueConfig = queueConfig;
        this.threadPoolSize = threadPoolSize;
        this.maxParallelMessages = Runtime.getRuntime().availableProcessors() * 2;

        // Create type-based message queues
        this.typeBasedQueues = new ConcurrentHashMap<>();

        // Initialize executors with custom thread factories
        this.primaryExecutor = Executors.newFixedThreadPool(threadPoolSize,
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger();

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("pipeline-worker-" + counter.incrementAndGet());
                        return thread;
                    }
                });

        this.completionExecutor = Executors.newFixedThreadPool(2,
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger();

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("completion-checker-" + counter.incrementAndGet());
                        return thread;
                    }
                });

        this.status = new AtomicReference<>(PipelineStatus.STOPPED);
    }

    @Override
    public void start() {
        if (status.get() != PipelineStatus.STOPPED) {
            throw new IllegalStateException("Pipeline is not in STOPPED state");
        }

        status.set(PipelineStatus.STARTING);
        startTypeBasedProcessors();
        status.set(PipelineStatus.RUNNING);
        logger.debug("Pipeline started successfully with {} worker threads", threadPoolSize);
    }

    private void startTypeBasedProcessors() {
        for (int i = 0; i < threadPoolSize; i++) {
            primaryExecutor.submit(this::processMessagesParallel);
        }
    }

    private void processMessagesParallel() {
        while (!Thread.currentThread().isInterrupted() && status.get() == PipelineStatus.RUNNING) {
            try {
                for (Map.Entry<String, ByteSizedBlockingQueue> entry : typeBasedQueues.entrySet()) {
                    String type = entry.getKey();
                    ByteSizedBlockingQueue queue = entry.getValue();

                    Message message = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        processMessageAsync(message)
                                .whenComplete((result, error) -> {
                                    if (error != null) {
                                        logger.error("Failed to process message {} of type {}: {}",
                                                message.getMsgOffset(), type, error.getMessage(), error);
                                    } else {
                                        logger.debug("Successfully processed message {} of type {}",
                                                message.getMsgOffset(), type);
                                    }
                                });
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in message processing loop", e);
            }
        }
    }

    private CompletableFuture<List<ProcessingResult>> processBatchParallel(List<Message> messages) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Group messages by type for efficient processing
                Map<String, List<Message>> messagesByType = messages.stream()
                        .collect(Collectors.groupingBy(Message::getType));

                // Process each type group in parallel
                List<CompletableFuture<ProcessingResult>> futures = messagesByType.values().stream()
                        .flatMap(typedMessages -> typedMessages.stream()
                                .map(this::processMessageAsync))
                        .collect(Collectors.toList());

                // Wait for all processing to complete
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .thenApply(v -> futures.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList()))
                        .join();

            } catch (Exception e) {
                logger.error("Batch processing failed", e);
                throw new ProcessingException(
                        "Failed to process message batch",
                        ErrorCode.BATCH_PROCESSING_ERROR.getCode(),
                        true,
                        e
                );
            }
        }, primaryExecutor);
    }

    private CompletableFuture<ProcessingResult> processMessageAsync(Message message) {
        logger.debug("Starting async processing of message: {}", message.getMsgOffset());

        return CompletableFuture.supplyAsync(() -> {
                    try {
                        // First store the message
                        logger.info("Storing message {}", message.getMsgOffset());
                        messageStore.store(message).join();
                        logger.info("Message {} stored successfully", message.getMsgOffset());

                        // Then process the message
                        logger.info("Processing message {}", message.getMsgOffset());
                        ProcessingResult result = messageProcessor.processMessage(message).join();
                        logger.info("Message {} processed successfully", message.getMsgOffset());

                        // Store the processing result
                        logger.info("Storing processing result for message {}", message.getMsgOffset());
                        messageStore.storeProcessingResult(result).join();
                        logger.info("Processing result stored for message {}", message.getMsgOffset());

                        return result;
                    } catch (Exception e) {
                        logger.error("Failed to process message {}: {}", message.getMsgOffset(), e.getMessage(), e);
                        throw new CompletionException(e);
                    }
                }, primaryExecutor)
                .orTimeout(300, TimeUnit.SECONDS)
                .exceptionally(throwable -> {
                    logger.error("Message processing failed for {}: {}", message.getMsgOffset(), throwable.getMessage());
                    throw new CompletionException(throwable);
                });
    }

    private CompletableFuture<Long> monitorMessageCompletion(Message message) {
        CompletableFuture<Long> resultFuture = new CompletableFuture<>();

        completionExecutor.execute(() -> {
            try {
                int attempts = 0;
                boolean completed = false;
                while (attempts < 60 && !completed) { // 60 second timeout
                    logger.debug("Checking completion - Attempt {} for message: {}",
                            attempts, message.getMsgOffset());

                    try {
                        CompletableFuture<Optional<ProcessingResult>> resultCheck =
                                messageStore.getProcessingResult(message.getMsgOffset());

                        Optional<ProcessingResult> result = resultCheck.get(2, TimeUnit.SECONDS);
                        if (result.isPresent()) {
                            logger.debug("Found processing result for message: {}", message.getMsgOffset());
                            if (result.get().isSuccessful()) {
                                logger.debug("Message processing confirmed successful: {}", message.getMsgOffset());
                                resultFuture.complete(message.getMsgOffset());
                                completed = true;
                            } else {
                                logger.error("Message processing failed according to result");
                                resultFuture.completeExceptionally(
                                        new ProcessingException(
                                                "Message processing failed",
                                                ErrorCode.PROCESSING_FAILED.getCode(),
                                                false
                                        )
                                );
                                completed = true;
                            }
                        } else {
                            // Check if message is still in queue
                            ByteSizedBlockingQueue queue = typeBasedQueues.get(message.getType());
                            if (queue != null && !queue.contains(message)) {
                                logger.debug("Message {} is no longer in queue, waiting for processing completion",
                                        message.getMsgOffset());
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error checking result: {} - will retry", e.getMessage());
                    }

                    if (!completed) {
                        Thread.sleep(1000);
                        attempts++;
                    }
                }

                if (!completed) {
                    logger.error("Message processing verification timed out after 60 seconds for message: {}",
                            message.getMsgOffset());
                    resultFuture.completeExceptionally(
                            new TimeoutException("Message processing verification timed out")
                    );
                }
            } catch (Exception e) {
                logger.error("Fatal error in completion checker: {}", e.getMessage());
                resultFuture.completeExceptionally(e);
            }
        });

        return resultFuture;
    }

    @Override
    public CompletableFuture<Long> submitMessage(Message message) {
        if (status.get() != PipelineStatus.RUNNING) {
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Pipeline is not running",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true
                    )
            );
        }

        // Get or create byte-sized queue for message type
        ByteSizedBlockingQueue typeQueue = typeBasedQueues.computeIfAbsent(
                message.getType(),
                k -> new ByteSizedBlockingQueue(
                        message.getType(),
                        queueConfig.getInitialQueueCapacity(),
                        queueConfig.getMaxQueueSizeBytes()
                )
        );

        try {
            logger.debug("Attempting to queue message with offset: {}", message.getMsgOffset());
            if (!typeQueue.offer(message, 5, TimeUnit.SECONDS)) {
                return CompletableFuture.failedFuture(
                        new ProcessingException(
                                "Failed to queue message - queue full",
                                ErrorCode.QUEUE_FULL.getCode(),
                                true
                        )
                );
            }
            logger.debug("Message successfully queued: {}", message.getMsgOffset());

            return monitorMessageCompletion(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Message submission interrupted",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true,
                            e
                    )
            );
        }
    }

    @Override
    public CompletableFuture<List<Long>> submitBatch(List<Message> messages) {
        if (status.get() != PipelineStatus.RUNNING) {
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Pipeline is not running",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true
                    )
            );
        }

        // Group messages by type
        Map<String, List<Message>> messagesByType = messages.stream()
                .collect(Collectors.groupingBy(Message::getType));

        // Submit each type group to its queue
        List<CompletableFuture<List<Long>>> futures = messagesByType.entrySet().stream()
                .map(entry -> submitTypeGroup(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        // Wait for all groups to complete
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .flatMap(future -> future.join().stream())
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<List<Long>> submitTypeGroup(String type, List<Message> messages) {
        ByteSizedBlockingQueue typeQueue = typeBasedQueues.computeIfAbsent(
                type,
                k -> new ByteSizedBlockingQueue(
                        type,
                        queueConfig.getInitialQueueCapacity(),
                        queueConfig.getMaxQueueSizeBytes()
                )
        );

        try {
            for (Message message : messages) {
                if (!typeQueue.offer(message, 5, TimeUnit.SECONDS)) {
                    return CompletableFuture.failedFuture(
                            new ProcessingException(
                                    "Failed to queue message batch - queue full",
                                    ErrorCode.QUEUE_FULL.getCode(),
                                    true
                            )
                    );
                }
            }

            return batchProcessor.processBatch(messages)
                    .thenApply(results -> results.stream()
                            .map(ProcessingResult::getOffset)
                            .collect(Collectors.toList()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void stop() {
        status.set(PipelineStatus.STOPPING);
        primaryExecutor.shutdown();
        completionExecutor.shutdown();
        try {
            if (!primaryExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                primaryExecutor.shutdownNow();
            }
            if (!completionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                completionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            primaryExecutor.shutdownNow();
            completionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        status.set(PipelineStatus.STOPPED);
        logger.debug("Pipeline stopped successfully");
    }

    @Override
    public PipelineStatus getStatus() {
        return status.get();
    }

    @Override
    public boolean canAccept() {
        if (status.get() != PipelineStatus.RUNNING ||
                !messageProcessor.canAccept() ||
                !messageStore.canAccept()) {
            return false;
        }

        // Check overall memory usage
        long totalUsedBytes = typeBasedQueues.values().stream()
                .mapToLong(ByteSizedBlockingQueue::getCurrentSizeBytes)
                .sum();

        // Check if we've exceeded total memory threshold
        long maxTotalBytes = queueConfig.getMaxQueueSizeBytes() * typeBasedQueues.size();
        if (totalUsedBytes >= maxTotalBytes) {
            logger.warn("Total queue memory usage exceeded: {}/{} bytes", totalUsedBytes, maxTotalBytes);
            return false;
        }

        // Check system memory
        Runtime runtime = Runtime.getRuntime();
        long freeMemory = runtime.freeMemory();
        if (freeMemory < queueConfig.getMinimumFreeMemoryBytes()) {
            logger.warn("System free memory too low: {} bytes", freeMemory);
            return false;
        }

        return true;
    }
}

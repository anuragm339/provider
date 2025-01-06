package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.*;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.models.Message;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class DefaultPipelineManager implements PipelineManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPipelineManager.class);

    private final MessageProcessor messageProcessor;
    private final BatchProcessor batchProcessor;
    private final MessageStore messageStore;
    private final ExecutorService executor;
    private final AtomicReference<PipelineStatus> status;
    private final BlockingQueue<Message> messageQueue;
    private final int queueCapacity;
    private final ExecutorService completionExecutor;

    public DefaultPipelineManager(
            MessageProcessor messageProcessor,
            BatchProcessor batchProcessor,
            MessageStore messageStore,
            ExecutorService executor,
            int queueCapacity,
            int threadPoolSize) {
        this.messageProcessor = messageProcessor;
        this.batchProcessor = batchProcessor;
        this.messageStore = messageStore;
        this.queueCapacity = queueCapacity;
        this.messageQueue = new LinkedBlockingQueue<>(queueCapacity);
        ThreadFactory processingThreadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("processing-thread-" + counter.incrementAndGet());
                return thread;
            }
        };

        ThreadFactory completionThreadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("completion-checker-" + counter.incrementAndGet());
                return thread;
            }
        };

        this.executor = executor;
        this.completionExecutor = Executors.newFixedThreadPool(2, completionThreadFactory);
        this.status = new AtomicReference<>(PipelineStatus.STOPPED);
    }

    @Override
    public void start() {
        if (status.get() != PipelineStatus.STOPPED) {
            throw new IllegalStateException("Pipeline is not in STOPPED state");
        }

        status.set(PipelineStatus.STARTING);
        startProcessing();
        status.set(PipelineStatus.RUNNING);
        logger.debug("Pipeline started successfully");
    }

    @Override
    public void stop() {
        status.set(PipelineStatus.STOPPING);
        executor.shutdown();
        completionExecutor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            if (!completionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                completionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            completionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        status.set(PipelineStatus.STOPPED);
        System.out.println("Pipeline stopped successfully");
    }

    private void startProcessing() {
        int processors = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < processors; i++) {
            executor.submit(this::processMessages);
        }
    }

    private void processMessages() {
        while (!Thread.currentThread().isInterrupted() &&
                status.get() == PipelineStatus.RUNNING) {
            try {
                //read the message from controller and start
                Message message = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    logger.debug("Processing message from queue with offset: {} " , message.getMsgOffset());
                    processMessage(message)
                            .thenAccept(result -> {

                                logger.debug("Successfully processed message:  {} " , message.getMsgOffset());
                            })
                            .exceptionally(throwable -> {
                               logger.error("Failed to process message: {} {} " , message.getMsgOffset() +
                                        ", Error: " + throwable.getMessage());
                                throwable.printStackTrace();
                                return null;
                            });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public CompletableFuture<Long> submitMessage(Message message) {
        if (status.get() != PipelineStatus.RUNNING) {
            System.out.println("Pipeline is not running. Current status: " + status.get());
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Pipeline is not running",
                            com.example.messaging.exceptions.ErrorCode.PROCESSING_FAILED.getCode(),
                            true
                    )
            );
        }

        if (!canAccept()) {
            System.out.println("Pipeline cannot accept messages. Queue size: " + messageQueue.size());
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Pipeline queue is full",
                            com.example.messaging.exceptions.ErrorCode.QUEUE_FULL.getCode(),
                            true
                    )
            );
        }

        CompletableFuture<Long> resultFuture = new CompletableFuture<>();

        try {
            logger.debug("Attempting to queue message with offset:  {} " , message.getMsgOffset());
            if (!messageQueue.offer(message, 5, TimeUnit.SECONDS)) {
                System.out.println("Failed to queue message after timeout");
                return CompletableFuture.failedFuture(
                        new ProcessingException(
                                "Failed to queue message",
                                com.example.messaging.exceptions.ErrorCode.QUEUE_FULL.getCode(),
                                true
                        )
                );
            }
            logger.debug("Message successfully queued:  {} " , message.getMsgOffset());

            // Start a completion checker
            logger.debug("Starting completion checker for message: {}" , message.getMsgOffset());
            completionExecutor.execute(() -> {
                try {
                    logger.debug("Completion checker started for message: {}" , message.getMsgOffset());
                    int attempts = 0;
                    boolean completed = false;
                    while (attempts < 30 && !completed) { // 30 second timeout
                        logger.debug("Checking completion - Attempt  {} for message:  {}" , message.getMsgOffset(),message);

                        try {
                            CompletableFuture<Optional<ProcessingResult>> resultCheck =
                                    messageStore.getProcessingResult(message.getMsgOffset());
                            logger.debug("Got result check future for message: {}" , message.getMsgOffset());

                            Optional<ProcessingResult> result = resultCheck.get(1, TimeUnit.SECONDS);
                            if (result.isPresent()) {
                                logger.debug("Found processing result for message:  {} " , message.getMsgOffset());
                                if (result.get().isSuccessful()) {
                                    logger.debug("Message processing confirmed successful: {} " , message.getMsgOffset());
                                    resultFuture.complete(message.getMsgOffset());
                                    completed = true;
                                } else {
                                    System.out.println("Message processing failed according to result");
                                    resultFuture.completeExceptionally(
                                            new ProcessingException(
                                                    "Message processing failed",
                                                    com.example.messaging.exceptions.ErrorCode.PROCESSING_FAILED.getCode(),
                                                    false
                                            )
                                    );
                                    completed = true;
                                }
                            } else {
                                logger.debug("No processing result found yet for message {}  attempt {} " + message.getMsgOffset() ,
                                         attempts);
                            }
                        } catch (Exception e) {
                            System.out.println("Error checking result: " + e.getMessage());
                            e.printStackTrace();
                        }

                        if (!completed) {
                            logger.debug("Waiting before next check attempt...");
                            Thread.sleep(1000);
                            attempts++;
                        }
                    }

                    if (!completed) {
                        System.out.println("Message processing verification timed out after 30 seconds");
                        resultFuture.completeExceptionally(
                                new TimeoutException("Message processing verification timed out")
                        );
                    }
                } catch (Exception e) {
                    System.out.println("Fatal error in completion checker: " + e.getMessage());
                    e.printStackTrace();
                    resultFuture.completeExceptionally(e);
                }
            });

            return resultFuture;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Message submission interrupted");
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Message submission interrupted",
                            com.example.messaging.exceptions.ErrorCode.PROCESSING_FAILED.getCode(),
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
                            true,
                            null
                    )
            );
        }

        if (!canAcceptBatch(messages.size())) {
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Pipeline cannot accept batch of size " + messages.size(),
                            ErrorCode.BATCH_SIZE_INVALID.getCode(),
                            true,
                            null
                    )
            );
        }

        return batchProcessor.processBatch(messages)
                .thenApply(results -> results.stream()
                        .map(ProcessingResult::getOffset)
                        .toList());
    }
    private boolean canAcceptBatch(int batchSize) {
        return status.get() == PipelineStatus.RUNNING &&
                batchProcessor.canAcceptBatch(batchSize) &&
                messageStore.canAccept();
    }

    private CompletableFuture<Long> processMessage(Message message) {
        return messageStore.store(message)
                .thenCompose(storedOffset -> {
                    logger.debug("Message stored with offset: {} " + storedOffset);

                    // Then process the message
                    return messageProcessor.processMessage(message)
                            .thenCompose(result -> {
                                logger.debug("Message processor completed for offset: {} " , message.getMsgOffset());
                                try {
                                    // Store the processing result
                                    logger.debug("Storing result for message: {} " , message.getMsgOffset());
                                    return messageStore.storeProcessingResult(result)
                                            .thenApply(v -> message.getMsgOffset());
                                } catch (Exception e) {
                                    logger.error("Failed to store result for message: {} ",  message.getMsgOffset());
                                    throw new ProcessingException(
                                            "Failed to store processing result",
                                            com.example.messaging.exceptions.ErrorCode.PROCESSING_FAILED.getCode(),
                                            true,
                                            e
                                    );
                                }
                            });
                })
                .exceptionally(throwable -> {
                    logger.error("Error processing message: " + throwable.getMessage());
                    throwable.printStackTrace();
                    throw new ProcessingException(
                            "Failed to process message",
                            com.example.messaging.exceptions.ErrorCode.PROCESSING_FAILED.getCode(),
                            true,
                            throwable
                    );
                });
    }

    @Override
    public PipelineStatus getStatus() {
        return status.get();
    }

    @Override
    public boolean canAccept() {
        return status.get() == PipelineStatus.RUNNING &&
                messageQueue.size() < queueCapacity &&
                messageProcessor.canAccept() &&
                messageStore.canAccept();
    }
}

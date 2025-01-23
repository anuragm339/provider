package com.example.messaging.core.pipeline.impl;


import com.example.messaging.models.Message;
import com.example.messaging.storage.db.sqlite.ConsumerOffsetTracker;
import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.handler.MessagePublisher;
import com.example.messaging.storage.service.MessageStore;

import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
public class MessageDispatchOrchestrator {
    private static final Logger logger = LoggerFactory.getLogger(MessageDispatchOrchestrator.class);

    // Dependencies
    private final MessageStore messageStore;
    private final ConsumerRegistry consumerRegistry;
    private final ConsumerOffsetTracker offsetTracker;
    private final DeadLetterQueueService deadLetterQueueService;
    private final MessagePublisher messagePublisher;

    // Configuration Parameters
    @Value("${message.dispatch.polling-interval-ms:5000}")
    private long pollingIntervalMs;

    @Value("${message.dispatch.batch-size:100}")
    private int batchSize;

    @Value("${message.dispatch.max-retry-count:1000}")
    private int maxRetryCount;

    @Value("${message.dispatch.retry-delay-ms:1000}")
    private long retryDelayMs;

    // Thread Management
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Thread dispatchThread;

    // Metrics Tracking
    private final ConcurrentMap<String, DispatchMetrics> groupMetrics = new ConcurrentHashMap<>();

    // Inner class for tracking dispatch metrics
    private static class DispatchMetrics {
        long totalMessagesSent = 0;
        long totalDispatchFailures = 0;
        long lastSuccessfulDispatchTime = 0;

        synchronized void incrementMessagesSent() {
            totalMessagesSent++;
            lastSuccessfulDispatchTime = System.currentTimeMillis();
        }

        synchronized void incrementDispatchFailures() {
            totalDispatchFailures++;
        }
    }

    // Constructor with all dependencies
    public MessageDispatchOrchestrator(
            MessageStore messageStore,
            ConsumerRegistry consumerRegistry,
            ConsumerOffsetTracker offsetTracker,
            DeadLetterQueueService deadLetterQueueService,
            MessagePublisher messagePublisher
    ) {
        this.messageStore = messageStore;
        this.consumerRegistry = consumerRegistry;
        this.offsetTracker = offsetTracker;
        this.deadLetterQueueService = deadLetterQueueService;
        this.messagePublisher = messagePublisher;
    }

    // Existing methods from previous implementation...

    /**
     * Dispatch messages for a specific consumer group
     * @param groupId Consumer group identifier
     * @return Mono<Void> representing the dispatch operation
     */
    public void dispatchToGroup(String groupId) {
        List<Message> unprocessedMessages = findUnprocessedMessagesForGroup(groupId);

        if (unprocessedMessages.isEmpty()) {
            return ;
        }

        dispatchMessageBatch(groupId, unprocessedMessages);
    }

    /**
     * Find unprocessed messages for a specific group
     * @param groupId Consumer group identifier
     * @return List of unprocessed messages
     */
    private List<Message> findUnprocessedMessagesForGroup(String groupId) {
        long lastProcessedOffset = offsetTracker.getLastProcessedOffset(groupId);
        String[] split = groupId.split("-");
        groupId="type-"+split[1];
        try {
            return messageStore.getMessagesAfterOffset(lastProcessedOffset,groupId)
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .limit(batchSize)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Failed to retrieve unprocessed messages", e);
            return Collections.emptyList();
        }
    }

    /**
     * Dispatch a batch of messages to a group
     *
     * @param groupId  Consumer group identifier
     * @param messages List of messages to dispatch
     * @return Mono<Void> representing the batch dispatch operation
     */
    private void dispatchMessageBatch(String groupId, List<Message> messages) {
        DispatchMetrics metrics = groupMetrics.computeIfAbsent(
                groupId,
                k -> new DispatchMetrics()
        );

        if (messages.isEmpty()) {
            logger.warn("No messages to dispatch for groupId: {}", groupId);
            return ;
        }

        logger.info("Dispatching message batch for groupId: {} with {} messages", groupId, messages.size());
        messages.stream().forEach(message -> {
            messagePublisher.publishMessage(message, groupId)
                    .doOnSubscribe(__ -> logger.debug("Starting publish for message {}", message.getMsgOffset()))
                    .doOnSuccess(__ -> {
                        logger.debug("Successfully published message {}", message.getMsgOffset());
                        metrics.incrementMessagesSent();
                        updateConsumerOffset(groupId, message);
                    })
                    .doOnError(error -> logger.error("Failed to publish message {}: {}", message.getMsgOffset(), error.getMessage()))
                    .block();
        });
    }



    /**
     * Dispatch a single message to a group
     * @param groupId Consumer group identifier
     * @param message Message to dispatch
     * @return Mono<Void> representing the single message dispatch
     */
    private Mono<Void> dispatchSingleMessage(String groupId, Message message) {
        logger.debug("Starting dispatch for message {} in group {}", message.getMsgOffset(), groupId);

        return messagePublisher.publishMessage(message, groupId)
                .timeout(Duration.ofSeconds(100))
                .doOnSuccess(__ -> {
                    logger.debug("Successfully dispatched message {} in group {}", message.getMsgOffset(), groupId);
                    updateConsumerOffset(groupId, message);
                })
                .onErrorResume(error -> {
                    if (error instanceof TimeoutException) {
                        logger.error("Dispatch timed out for message {} in group {}", message.getMsgOffset(), groupId, error);
                    } else {
                        logger.error("Failed to dispatch message {} in group {}", message.getMsgOffset(), groupId, error);
                    }
                    return Mono.empty();
                })
                .doOnTerminate(() -> logger.debug("Completed processing for message {} in group {}", message.getMsgOffset(), groupId));
    }


    /**
     * Update consumer offset after successful dispatch
     * @param groupId Consumer group identifier
     * @param message Dispatched message
     */
    private void updateConsumerOffset(String groupId, Message message) {
        consumerRegistry.getActiveConsumersInGroup(groupId)
                .stream()
                .findFirst()
                .ifPresent(consumer ->
                        offsetTracker.updateConsumerOffset(
                                consumer.getMetadata().getConsumerId(),
                                groupId,
                                message.getMsgOffset()
                        )
                );
    }

    /**
     * Handle dispatch errors
     * @param error Error during dispatch
     * @return Mono<Void> for error handling
     */
    private Mono handleDispatchError(Throwable error) {
        logger.error("Dispatch error", error);
        return Mono.empty();
    }

    /**
     * Handle individual message dispatch failure
     * @param groupId Consumer group identifier
     * @param message Failed message
     * @param error Dispatch error
     */
    private void handleMessageDispatchFailure(
            String groupId,
            Message message,
            Throwable error
    ) {
        // Move to Dead Letter Queue if max retries exceeded
        if (message.getRetryCount() >= maxRetryCount) {
            deadLetterQueueService.enqueue(
                    message,
                    "Dispatch failed after " + maxRetryCount + " attempts: " + error.getMessage()
            );
        } else {
            // Retry mechanism
            Message retriedMessage = Message
                    .from(message)
                    .retryCount(message.getRetryCount() + 1)
                    .build();

            // Schedule retry with backoff
            scheduleRetry(groupId, retriedMessage);
        }
    }

    /**
     * Schedule message retry with exponential backoff
     * @param groupId Consumer group identifier
     * @param message Message to retry
     */
    private void scheduleRetry(String groupId, Message message) {
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(calculateBackoffDelay(message.getRetryCount()));
                dispatchSingleMessage(groupId, message).block();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Calculate backoff delay with jitter
     * @param retryCount Number of retry attempts
     * @return Backoff delay in milliseconds
     */
    private long calculateBackoffDelay(int retryCount) {
        long baseDelay = retryDelayMs;
        long delay = baseDelay * (long) Math.pow(2, retryCount);
        double jitter = 1 + (Math.random() * 0.5);
        return (long) (delay * jitter);
    }

    /**
     * Get dispatch metrics for a specific group
     * @param groupId Consumer group identifier
     * @return DispatchMetrics for the group
     */
    public DispatchMetrics getGroupMetrics(String groupId) {
        return groupMetrics.getOrDefault(groupId, new DispatchMetrics());
    }

    /**
     * Clear metrics for a specific group
     * @param groupId Consumer group identifier
     */
    public void clearGroupMetrics(String groupId) {
        groupMetrics.remove(groupId);
    }

    @PostConstruct
    public synchronized void start() {
        // Ensure we're not already running
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Starting Message Dispatch Orchestrator");

            // Create and configure the dispatch thread
            dispatchThread = new Thread(this::dispatchLoop, "message-dispatch-orchestrator");
            dispatchThread.setPriority(Thread.MAX_PRIORITY);
            dispatchThread.setUncaughtExceptionHandler((thread, throwable) -> {
                logger.error("Uncaught exception in dispatch thread", throwable);
                stop(); // Stop the orchestrator if there's an unhandled exception
            });

            // Start the thread
            dispatchThread.start();
        } else {
            logger.warn("Message Dispatch Orchestrator is already running");
        }
    }

    @PreDestroy
    public synchronized void stop() {
        // Ensure we're currently running
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping Message Dispatch Orchestrator");

            // Interrupt the dispatch thread
            if (dispatchThread != null) {
                dispatchThread.interrupt();

                try {
                    // Wait for the thread to terminate
                    dispatchThread.join(10000); // 10-second timeout
                } catch (InterruptedException e) {
                    // Restore interrupt status
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for dispatch thread to stop");
                }

                // Force shutdown if thread doesn't terminate
                if (dispatchThread.isAlive()) {
                    logger.warn("Dispatch thread did not terminate gracefully");
                    dispatchThread.stop(); // Last resort, not recommended in production
                }
            }

            // Clear any resources or perform cleanup
            clearResources();

            logger.info("Message Dispatch Orchestrator stopped");
        } else {
            logger.warn("Message Dispatch Orchestrator is not running");
        }
    }

    /**
     * Clear resources and reset state
     */
    private void clearResources() {
        // Clear metrics
        groupMetrics.clear();
    }

    /**
     * Main dispatch loop
     * Runs continuously when the orchestrator is active
     */
    private void dispatchLoop() {
        while (isRunning.get()) {
            try {
                // Find all active consumer groups
                Set<String> activeConsumerGroups = consumerRegistry.findActiveConsumerGroups();

                // Process each consumer group
                activeConsumerGroups.forEach(this::processConsumerGroup);

                // Sleep between polling cycles
                Thread.sleep(pollingIntervalMs);
            } catch (InterruptedException e) {
                // Restore interrupt status and break the loop
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Log and handle unexpected errors
                handleDispatchLoopException(e);
            }
        }
    }

    private void processConsumerGroup(String s) {
        dispatchToGroup(s);
    }

    /**
     * Handle exceptions in the dispatch loop
     * @param e Exception encountered
     */
    private void handleDispatchLoopException(Exception e) {
        logger.error("Error in dispatch loop", e);

        try {
            // Exponential backoff
            long backoffTime = Math.min(pollingIntervalMs * 2, 60000); // Max 1 minute
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            isRunning.set(false);
        }
    }
}

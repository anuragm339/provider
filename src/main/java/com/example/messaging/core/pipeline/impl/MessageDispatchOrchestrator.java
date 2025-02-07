package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.model.BatchStatus;
import com.example.messaging.models.BatchMessage;
import com.example.messaging.models.Message;
import com.example.messaging.models.MessageState;
import com.example.messaging.monitoring.metrics.ConsumerPerformanceTracker;
import com.example.messaging.storage.db.sqlite.ConsumerOffsetTracker;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.handler.MessagePublisher;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Context
@Singleton
public class MessageDispatchOrchestrator {
    private static final Logger logger = LoggerFactory.getLogger(MessageDispatchOrchestrator.class);

    private final MessageStore messageStore;
    private final ConsumerRegistry consumerRegistry;
    private final ConsumerOffsetTracker offsetTracker;
    private final MessagePublisher messagePublisher;
    private final Map<String, BatchStatus> batchStatuses = new ConcurrentHashMap<>();
    private final AtomicLong batchSequencer = new AtomicLong(0);

    @Value("${message.dispatch.batch-timeout-ms:30000}")
    private long batchTimeoutMs;

    @Value("${message.dispatch.max-retries:3}")
    private int maxRetries;

    private ConsumerPerformanceTracker performanceTracker;
    private final Map<String, MessageDeliveryContext> deliveryContexts = new ConcurrentHashMap<>();

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;
    @Value("${message.store.batch-size:16}")
    private int defaultBatchSize;

    public MessageDispatchOrchestrator(
            MessageStore messageStore,
            ConsumerRegistry consumerRegistry,
            ConsumerOffsetTracker offsetTracker,
            MessagePublisher messagePublisher, ConsumerPerformanceTracker performanceTracker) {
        this.messageStore = messageStore;
        this.consumerRegistry = consumerRegistry;
        this.offsetTracker = offsetTracker;
        this.messagePublisher = messagePublisher;
        this.performanceTracker=performanceTracker;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("msg-dispatch-scheduler-%d")
                .build();
        // Set fixed core pool size to prevent thread growth
        this.scheduler = new ScheduledThreadPoolExecutor(2, threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()){
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                logger.debug("Executing task on thread: {}", t.getName());
            }
        };
        // Prevent core threads timeout
        ((ScheduledThreadPoolExecutor) scheduler).setRemoveOnCancelPolicy(true);
        ((ScheduledThreadPoolExecutor) scheduler).setMaximumPoolSize(2);
    }

    public void dispatchToGroup(String groupId) {
        if (!consumerRegistry.hasActiveConsumers(groupId)) {
            logger.debug("No active consumers for group {}", groupId);
            return;
        }

        if (hasPendingBatch(groupId)) {
            logger.debug("Skipping dispatch for group {} as it has pending batch", groupId);
            return;
        }

        List<Message> unprocessedMessages = findUnprocessedMessagesForGroup(groupId);
        if (!unprocessedMessages.isEmpty()) {
            // Calculate total size for metrics
            long totalSize = unprocessedMessages.stream()
                    .mapToLong(msg -> msg.getData().length)
                    .sum();

            // Store delivery context
            deliveryContexts.put(groupId, new MessageDeliveryContext(
                    System.currentTimeMillis(),
                    totalSize,
                    unprocessedMessages.size()
            ));

            dispatchMessageBatch(groupId, unprocessedMessages);
        }
    }

    private boolean hasPendingBatch(String groupId) {
        return batchStatuses.values().stream()
                .anyMatch(status ->
                        status.getGroupId().equals(groupId) &&
                                !status.isComplete() &&
                                !status.isExpired(batchTimeoutMs)
                );
    }

    private List<Message> findUnprocessedMessagesForGroup(String groupId) {
        long lastProcessedOffset = offsetTracker.getLastProcessedOffset(groupId);

        try {
            List<Message> messagesAfterOffset = messageStore.getMessagesAfterOffset(lastProcessedOffset, groupId).join();
            logger.info("Found {} unprocessed messages for group {}", messagesAfterOffset.size(), groupId);
            return messagesAfterOffset;
        } catch (Exception e) {
            logger.error("Failed to retrieve unprocessed messages", e);
            return Collections.emptyList();
        }
    }

    private void dispatchMessageBatch(String groupId, List<Message> messages) {
        String batchId = generateBatchId(groupId);
        long lastSuccessfulOffset = offsetTracker.getLastProcessedOffset(groupId);
        BatchMessage batchMessage = new BatchMessage(batchId, (long) messages.size(), messages, groupId);
        List<Long> collect = messages.stream().map(Message::getMsgOffset).collect(Collectors.toList());
        // Create and store batch status before publishing
        BatchStatus batchStatus = new BatchStatus(batchId, groupId, messages.size(), collect, lastSuccessfulOffset,defaultBatchSize);
        batchStatuses.put(batchId, batchStatus);

        //logger.info("Created new batch {} for group {} with {} messages", batchId, groupId, messages.size());

        // Schedule batch timeout check
        scheduler.schedule(
                () -> checkBatchTimeout(batchId),
                batchTimeoutMs,
                TimeUnit.MILLISECONDS
        );

        messageStore.updateMessageStatus(collect, MessageState.SENT, groupId).join();
        messagePublisher.publishBatchMessage(batchMessage, groupId)
                .doOnSuccess(__ -> {
                    logger.debug("Published batch {} for group {}", batchId, groupId);
                })
                .doOnError(error -> {
                    logger.error("Failed to publish batch {} for group {}: {}", batchId, groupId, error.getMessage());
                    handleDeliveryFailure(groupId, batchId, error);
                    cleanupBatch(batchId);
                })
                .block();
    }

    private String generateBatchId(String groupId) {
        return String.format("%s-%d-%d",
                groupId,
                System.currentTimeMillis(),
                batchSequencer.incrementAndGet()
        );
    }

    private void checkBatchTimeout(String batchId) {
        BatchStatus status = batchStatuses.get(batchId);
        if (status != null && !status.isComplete() && status.isExpired(batchTimeoutMs)) {
            handleBatchTimeout(status);
        }
    }

    private void handleBatchTimeout(BatchStatus status) {
        if (!consumerRegistry.hasActiveConsumers(status.getGroupId())) {
            logger.debug("No active consumers for group {}, skipping retry for batch {}",
                    status.getGroupId(), status.getBatchId());
            cleanupBatch(status.getBatchId());
            return;
        }

        if (status.getRetryCount() >= maxRetries) {
            logger.error("Batch {} exceeded max retries. Moving to DLQ.", status.getBatchId());
            offsetTracker.updateConsumerOffset(
                    status.getGroupId(),
                    status.getGroupId(),
                    status.getPreviousSuccessfulBatchOffset()
            );
            cleanupBatch(status.getBatchId());
        } else {
            retryBatch(status);
        }
    }

    private void retryBatch(BatchStatus status) {
        List<Long> unackedOffsets = status.getUnacknowledgedOffsets();
        if (!unackedOffsets.isEmpty() && consumerRegistry.hasActiveConsumers(status.getGroupId())) {
            try {
                List<Message> messages = messageStore.getMessagesByOffsets(unackedOffsets)
                        .get(10, TimeUnit.SECONDS);

                status.incrementRetryCount();
                logger.info("Retrying batch {} (attempt {}) for group {}",
                        status.getBatchId(), status.getRetryCount(), status.getGroupId());

                dispatchMessageBatch(status.getGroupId(), messages);
            } catch (Exception e) {
                logger.error("Failed to retry batch {}", status.getBatchId(), e);
                cleanupBatch(status.getBatchId());
            }
        } else {
            logger.debug("No unacked messages or active consumers for batch {}, cleaning up",
                    status.getBatchId());
            cleanupBatch(status.getBatchId());
        }
    }

    public Mono<Void> handleBatchAcknowledgment(String batchId, List<Long> offsets, String consumerId) {
        BatchStatus status = batchStatuses.get(batchId);
        if (status != null) {
            // Record delivery metrics
            MessageDeliveryContext context = deliveryContexts.remove(status.getGroupId());
            if (context != null) {
                long deliveryTime = System.currentTimeMillis() - context.startTime;
                performanceTracker.recordBatchSent(
                        status.getGroupId(),
                        context.messageCount,
                        context.totalSize,
                        deliveryTime
                );
            }

            messageStore.updateMessageStatus(offsets, MessageState.DELIVERED, consumerId).join();
            status.acknowledgeMessage(offsets.get(offsets.size()-1), consumerId);
            updateConsumerOffsets(status);
            cleanupBatch(batchId);
        }
        return Mono.empty();
    }

    private void updateConsumerOffsets(BatchStatus status) {
        if (status.isComplete()) {
            long maxOffset = getMaxOffset(status);
            if (maxOffset > status.getPreviousSuccessfulBatchOffset()) {
                offsetTracker.updateConsumerOffset(
                        status.getGroupId(),
                        status.getGroupId(),
                        maxOffset
                );
                logger.info("Updated consumer offset for group {} to {}", status.getGroupId(), maxOffset);
            } else {
                logger.warn("Batch {} has non-contiguous offsets, not updating consumer offset", status.getBatchId());
            }
        } else {
            offsetTracker.updateConsumerOffset(
                    status.getGroupId(),
                    status.getGroupId(),
                    status.getPreviousSuccessfulBatchOffset()
            );
            logger.warn("Batch {} was not complete, reset to previous offset {}",
                    status.getBatchId(), status.getPreviousSuccessfulBatchOffset());
        }
    }

    private long getMaxOffset(BatchStatus status) {
        return status.getUnacknowledgedOffsets().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
    }

    private void handleDeliveryFailure(String groupId, String batchId, Throwable error) {
        logger.error("Failed to deliver batch {} to group {}: {}",
                batchId, groupId, error.getMessage());

        // Remove delivery context and record failure
        deliveryContexts.remove(groupId);
        performanceTracker.recordFailedDelivery(groupId);

        // Existing error handling logic...
    }

    private void cleanupBatch(String batchId) {
        BatchStatus status = batchStatuses.get(batchId);
        if (status != null) {
            // Remove any lingering delivery context
            deliveryContexts.remove(status.getGroupId());
        }
        batchStatuses.remove(batchId);
        logger.debug("Cleaned up batch {}", batchId);
    }

    @PostConstruct
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Starting Message Dispatch Orchestrator");

            scheduler.scheduleAtFixedRate(
                    this::processPendingMessages,
                    0,
                    1000,
                    TimeUnit.MILLISECONDS
            );

            scheduler.scheduleAtFixedRate(
                    this::cleanupExpiredBatches,
                    batchTimeoutMs,
                    batchTimeoutMs / 2,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private void processPendingMessages() {
        if (!isRunning.get()) {
            return;
        }

        try {
            Set<String> activeGroups = consumerRegistry.findActiveConsumerGroups();
            logger.debug("Processing messages for {} active consumer groups", activeGroups.size());

            for (String groupId : activeGroups) {
                try {
                    dispatchToGroup(groupId);
                } catch (Exception e) {
                    logger.error("Error processing messages for group {}", groupId, e);
                }
            }
        } catch (Exception e) {
            logger.error("Error in message processing loop", e);
        }
    }

    private void cleanupExpiredBatches() {
        if (!isRunning.get()) {
            return;
        }

        try {
            List<BatchStatus> expiredBatches = batchStatuses.values().stream()
                    .filter(status -> status.isExpired(batchTimeoutMs))
                    .collect(Collectors.toList());

            for (BatchStatus status : expiredBatches) {
                if (consumerRegistry.hasActiveConsumers(status.getGroupId())) {
                    handleBatchTimeout(status);
                } else {
                    cleanupBatch(status.getBatchId());
                }
            }
        } catch (Exception e) {
            logger.error("Error cleaning up expired batches", e);
        }
    }

    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Stopping Message Dispatch Orchestrator");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public Map<String, Object> getBatchMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalBatches", batchStatuses.size());
        metrics.put("completedBatches", batchStatuses.values().stream()
                .filter(BatchStatus::isComplete)
                .count());
        metrics.put("expiredBatches", batchStatuses.values().stream()
                .filter(s -> s.isExpired(batchTimeoutMs))
                .count());
        return metrics;
    }

    private static class MessageDeliveryContext {
        final long startTime;
        final long totalSize;
        final int messageCount;

        MessageDeliveryContext(long startTime, long totalSize, int messageCount) {
            this.startTime = startTime;
            this.totalSize = totalSize;
            this.messageCount = messageCount;
        }
    }

    @Scheduled(fixedDelay = "5m")
    public void cleanupStaleDeliveryContexts() {
        long staleThreshold = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
        deliveryContexts.entrySet().removeIf(entry -> {
            if (entry.getValue().startTime < staleThreshold) {
                performanceTracker.recordFailedDelivery(entry.getKey());
                return true;
            }
            return false;
        });
    }

    // Add method to get current dispatch metrics
    public Map<String, Object> getDispatchMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("activeDeliveries", deliveryContexts.size());
        metrics.put("pendingBatches", batchStatuses.size());
        metrics.put("performanceMetrics", performanceTracker.getGlobalSnapshot());
        return metrics;
    }

    public void cleanupConsumerResources(String consumerId, String groupId) {
        // Clear any pending batches for this consumer
        batchStatuses.entrySet().removeIf(entry -> {
            BatchStatus status = entry.getValue();
            if (status.getGroupId().equals(groupId)) {
                cleanupBatch(entry.getKey());
                return true;
            }
            return false;
        });

        // Clear delivery contexts
        deliveryContexts.remove(groupId);

        // Reset any in-progress message states
        resetInProgressMessages(groupId);
    }

    private void resetInProgressMessages(String groupId) {
        try {
            // Get all IN_PROGRESS messages for this group
            List<Message> inProgressMessages = messageStore.getMessagesInState(groupId, MessageState.SENT).join();

            if (!inProgressMessages.isEmpty()) {
                // Reset their states to PENDING
                List<Long> offsets = inProgressMessages.stream().map(Message::getMsgOffset).collect(Collectors.toList());

                messageStore.updateMessageStatus(offsets, MessageState.PENDING, null).join();

                logger.info("Reset {} in-progress messages to pending for group {}", inProgressMessages.size(), groupId);
            }
        } catch (Exception e) {
            logger.error("Failed to reset in-progress messages for group {}: {}", groupId, e.getMessage(), e);
        }
    }
}

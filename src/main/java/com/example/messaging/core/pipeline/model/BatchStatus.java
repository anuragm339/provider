package com.example.messaging.core.pipeline.model;

import com.example.messaging.models.BatchState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BatchStatus {
    private final String batchId;
    private final String groupId;
    private final int totalMessages;
    private final Map<Long, MessageStatus> messageStatuses = new ConcurrentHashMap<>();

    public Map<Long, MessageStatus> getMessageStatuses() {
        return Collections.unmodifiableMap(messageStatuses);
    }
    private final AtomicInteger acknowledgedCount ;
    private volatile BatchState state = BatchState.PENDING;
    private final Instant createdAt;
    private final long previousSuccessfulBatchOffset;  // Track last successful batch offset
    private Instant completedAt;
    private int retryCount = 0;

    public BatchStatus(String batchId, String groupId, int totalMessages, List<Long> messageOffsets, long previousSuccessfulBatchOffset,int defaultBatchSize) {
        this.batchId = batchId;
        this.groupId = groupId;
        this.totalMessages = totalMessages;
        this.createdAt = Instant.now();
        this.previousSuccessfulBatchOffset = previousSuccessfulBatchOffset;
        acknowledgedCount=new AtomicInteger(defaultBatchSize);

        // Initialize message statuses
        messageOffsets.forEach(offset ->
                messageStatuses.put(offset, new MessageStatus(offset))
        );
    }

    public synchronized void acknowledgeMessage(long offset, String consumerId) {
        MessageStatus status = messageStatuses.get(offset);
        if (status != null && !status.isAcknowledged()) {
            status.setAcknowledged(true);
            status.setAckTime(Instant.now());
            status.setConsumerId(consumerId);

            if (acknowledgedCount.get() == totalMessages) {
                state = BatchState.COMPLETE;
                completedAt = Instant.now();
            }
        }
    }

    public boolean isComplete() {
        return state == BatchState.COMPLETE;
    }

    public List<Long> getUnacknowledgedOffsets() {
        return messageStatuses.entrySet().stream()
                .filter(e -> !e.getValue().isAcknowledged())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public boolean isExpired(long timeoutMs) {
        return Instant.now().isAfter(createdAt.plusMillis(timeoutMs));
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    // Getters
    public String getBatchId() { return batchId; }
    public String getGroupId() { return groupId; }
    public int getTotalMessages() { return totalMessages; }
    public int getAcknowledgedCount() { return acknowledgedCount.get(); }
    public BatchState getState() { return state; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getCompletedAt() { return completedAt; }
    public int getRetryCount() { return retryCount; }

    public void setState(BatchState state) {
        this.state = state;
    }

    public long getPreviousSuccessfulBatchOffset() {
        return previousSuccessfulBatchOffset;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    // MessageStatus inner class
}

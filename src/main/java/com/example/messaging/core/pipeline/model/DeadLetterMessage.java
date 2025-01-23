package com.example.messaging.core.pipeline.model;

import com.example.messaging.models.Message;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a message in the Dead Letter Queue
 */
public  class DeadLetterMessage {
    private String id;
    private Message originalMessage;
    private String errorReason;
    private Instant timestamp;
    private int retryCount;

    // Constructors
    public DeadLetterMessage() {}

    public DeadLetterMessage(Message originalMessage, String errorReason) {
        this.id = UUID.randomUUID().toString();
        this.originalMessage = originalMessage;
        this.errorReason = errorReason;
        this.timestamp = Instant.now();
        this.retryCount = 0;
    }

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public Message getOriginalMessage() { return originalMessage; }
    public void setOriginalMessage(Message originalMessage) { this.originalMessage = originalMessage; }
    public String getErrorReason() { return errorReason; }
    public void setErrorReason(String errorReason) { this.errorReason = errorReason; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
}

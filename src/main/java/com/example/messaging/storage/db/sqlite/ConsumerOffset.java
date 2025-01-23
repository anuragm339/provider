package com.example.messaging.storage.db.sqlite;

import java.time.Instant;

public class ConsumerOffset {
    private long offset;
    private Instant lastUpdated;
    private String consumerId;
    private String groupId;

    // Constructors, getters, and setters
    public ConsumerOffset() {}

    public ConsumerOffset(long offset, String consumerId, String groupId) {
        this.offset = offset;
        this.lastUpdated = Instant.now();
        this.consumerId = consumerId;
        this.groupId = groupId;
    }

    // Getters and setters
    public long getOffset() { return offset; }
    public void setOffset(long offset) {
        this.offset = offset;
        this.lastUpdated = Instant.now();
    }
    public Instant getLastUpdated() { return lastUpdated; }
    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }
}

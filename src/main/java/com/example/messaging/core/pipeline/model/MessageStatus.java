package com.example.messaging.core.pipeline.model;

import java.time.Instant;

public class MessageStatus {
    private final long offset;
    private boolean acknowledged;
    private Instant ackTime;
    private String consumerId;

    public MessageStatus(long offset) {
        this.offset = offset;
        this.acknowledged = false;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public void setAckTime(Instant ackTime) {
        this.ackTime = ackTime;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public long getOffset() { return offset; }
    public Instant getAckTime() { return ackTime; }
    public String getConsumerId() { return consumerId; }
}

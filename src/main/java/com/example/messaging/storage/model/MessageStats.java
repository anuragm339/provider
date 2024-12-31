package com.example.messaging.storage.model;

import java.util.Collections;
import java.util.Map;

public class MessageStats {
    private final long totalMessages;
    private final double averageMessageSize;
    private final Map<String, Long> messageCountByType;
    private final double averageProcessingTime;
    private final double averageWriteTime;
    private final double averageReadTime;
    private final long totalProcessed;

    private MessageStats(Builder builder) {
        this.totalMessages = builder.totalMessages;
        this.averageMessageSize = builder.averageMessageSize;
        this.messageCountByType = Collections.unmodifiableMap(builder.messageCountByType);
        this.averageProcessingTime = builder.averageProcessingTime;
        this.averageWriteTime = builder.averageWriteTime;
        this.averageReadTime = builder.averageReadTime;
        this.totalProcessed = builder.totalProcessed;
    }

    public long getTotalMessages() { return totalMessages; }
    public double getAverageMessageSize() { return averageMessageSize; }
    public Map<String, Long> getMessageCountByType() { return messageCountByType; }
    public double getAverageProcessingTime() { return averageProcessingTime; }
    public double getAverageWriteTime() { return averageWriteTime; }
    public double getAverageReadTime() { return averageReadTime; }
    public long getTotalProcessed() { return totalProcessed; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long totalMessages;
        private double averageMessageSize;
        private Map<String, Long> messageCountByType = Collections.emptyMap();
        private double averageProcessingTime;
        private double averageWriteTime;
        private double averageReadTime;
        private long totalProcessed;

        public Builder totalMessages(long totalMessages) {
            this.totalMessages = totalMessages;
            return this;
        }

        public Builder averageMessageSize(double averageMessageSize) {
            this.averageMessageSize = averageMessageSize;
            return this;
        }

        public Builder messageCountByType(Map<String, Long> messageCountByType) {
            this.messageCountByType = messageCountByType;
            return this;
        }

        public Builder averageProcessingTime(double averageProcessingTime) {
            this.averageProcessingTime = averageProcessingTime;
            return this;
        }

        public Builder averageWriteTime(double averageWriteTime) {
            this.averageWriteTime = averageWriteTime;
            return this;
        }

        public Builder averageReadTime(double averageReadTime) {
            this.averageReadTime = averageReadTime;
            return this;
        }

        public Builder totalProcessed(long totalProcessed) {
            this.totalProcessed = totalProcessed;
            return this;
        }

        public MessageStats build() {
            return new MessageStats(this);
        }
    }
}

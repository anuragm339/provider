package com.example.messaging.core.pipeline.model;

import com.example.messaging.models.Message;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class MessageBatch {
    private final String batchId;
    private final List<Message> messages;
    private final Instant createdAt;
    private final int batchSize;
    private final long startOffset;
    private final long endOffset;

    private MessageBatch(Builder builder) {
        this.batchId = builder.batchId;
        this.messages = List.copyOf(builder.messages);
        this.createdAt = builder.createdAt;
        this.batchSize = builder.messages.size();
        this.startOffset = calculateStartOffset(builder.messages);
        this.endOffset = calculateEndOffset(builder.messages);
    }

    private long calculateStartOffset(List<Message> messages) {
        return messages.stream()
                .mapToLong(Message::getMsgOffset)
                .min()
                .orElse(-1);
    }

    private long calculateEndOffset(List<Message> messages) {
        return messages.stream()
                .mapToLong(Message::getMsgOffset)
                .max()
                .orElse(-1);
    }

    public String getBatchId() { return batchId; }
    public List<Message> getMessages() { return messages; }
    public Instant getCreatedAt() { return createdAt; }
    public int getBatchSize() { return batchSize; }
    public long getStartOffset() { return startOffset; }
    public long getEndOffset() { return endOffset; }

    public static class Builder {
        private String batchId;
        private List<Message> messages;
        private Instant createdAt;

        public Builder messages(List<Message> messages) {
            this.messages = messages;
            return this;
        }

        public Builder batchId(String batchId) {
            this.batchId = batchId;
            return this;
        }

        public MessageBatch build() {
            if (batchId == null) {
                batchId = UUID.randomUUID().toString();
            }
            if (createdAt == null) {
                createdAt = Instant.now();
            }
            return new MessageBatch(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}

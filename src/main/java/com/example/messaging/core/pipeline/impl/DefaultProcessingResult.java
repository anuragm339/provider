package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.ProcessingResult;
import jakarta.inject.Singleton;

import java.time.Instant;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.util.Base64;

@Singleton
public class DefaultProcessingResult implements ProcessingResult {
    private final long offset;
    private final String integrityToken;
    private final long processingTimestamp;
    private final boolean successful;
    private final String checksum;

    private DefaultProcessingResult(Builder builder) {
        this.offset = builder.offset;
        this.processingTimestamp = builder.processingTimestamp;
        this.successful = builder.successful;
        this.checksum = builder.checksum;
        this.integrityToken = generateIntegrityToken();
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public String getIntegrityToken() {
        return integrityToken;
    }

    @Override
    public long getProcessingTimestamp() {
        return processingTimestamp;
    }

    @Override
    public boolean isSuccessful() {
        return successful;
    }

    @Override
    public String getChecksum() {
        return checksum;
    }

    private String generateIntegrityToken() {
        try {
            // Combine all fields for integrity token
            ByteBuffer buffer = ByteBuffer.allocate(256);
            buffer.putLong(offset);
            buffer.putLong(processingTimestamp);
            buffer.put(checksum.getBytes());
            buffer.put((byte) (successful ? 1 : 0));

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(buffer.array());
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate integrity token", e);
        }
    }

    public static class Builder {
        private long offset;
        private long processingTimestamp;
        private boolean successful;
        private String checksum;

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder processingTimestamp(long timestamp) {
            this.processingTimestamp = timestamp;
            return this;
        }

        public Builder successful(boolean successful) {
            this.successful = successful;
            return this;
        }

        public Builder checksum(String checksum) {
            this.checksum = checksum;
            return this;
        }

        public DefaultProcessingResult build() {
            if (processingTimestamp == 0) {
                processingTimestamp = Instant.now().toEpochMilli();
            }
            if (checksum == null) {
                throw new IllegalStateException("Checksum is required");
            }
            return new DefaultProcessingResult(this);
        }
    }
}

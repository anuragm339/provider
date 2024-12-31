package com.example.messaging.storage.service;

public interface StorageConfig {
    /**
     * Get maximum storage size in bytes
     */
    long getMaxStorageSize();

    /**
     * Get maximum number of messages to store
     */
    int getMaxMessages();

    /**
     * Get retention period in milliseconds
     */
    long getRetentionPeriodMs();

    /**
     * Get batch size for storage operations
     */
    int getBatchSize();

    /**
     * Get storage path
     */
    String getStoragePath();

    /**
     * Get true if compression is enabled
     */
    boolean isCompressionEnabled();

    /**
     * Get sync mode for writes
     */
    SyncMode getSyncMode();

    enum SyncMode {
        SYNC,       // Synchronous writes
        ASYNC,      // Asynchronous writes
        BATCH_SYNC  // Sync after batch
    }
}

package com.example.messaging.storage.db.sqlite;

import com.example.messaging.storage.service.StorageConfig;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;

@Singleton
public class SQLiteConfig implements StorageConfig {
    private final String dbPath;
    private final int maxMessages;
    private final long maxStorageSize;
    private final long retentionPeriodMs;
    private final int batchSize;
    private final boolean compressionEnabled;
    private final SyncMode syncMode;

    public SQLiteConfig(@Value("${sqlite.db-path}") String dbPath, @Value("${sqlite.maxMessages}") int maxMessages,
                        @Value("${sqlite.maxStorageSize}") long maxStorageSize, @Value("${sqlite.retentionPeriodMs}") long retentionPeriodMs,
                        @Value("${sqlite.maxPoolSize}") int batchSize, @Value("${sqlite.compressionEnabled}")boolean compressionEnabled) {
        this.dbPath = dbPath;
        this.maxMessages = maxMessages;
        this.maxStorageSize = maxStorageSize;
        this.retentionPeriodMs = retentionPeriodMs;
        this.batchSize = batchSize;
        this.compressionEnabled = compressionEnabled;
        this.syncMode = SyncMode.BATCH_SYNC;
    }

    @Override
    public long getMaxStorageSize() {
        return maxStorageSize;
    }

    @Override
    public int getMaxMessages() {
        return maxMessages;
    }

    @Override
    public long getRetentionPeriodMs() {
        return retentionPeriodMs;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public String getStoragePath() {
        return dbPath;
    }

    @Override
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    @Override
    public SyncMode getSyncMode() {
        return syncMode;
    }

    public String getDbPath() {
        return dbPath;
    }
}

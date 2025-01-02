package com.example.messaging.storage.db.rocks.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.inject.Singleton;

@Singleton
public class RocksProperties {
    private Storage storage = new Storage();
    private Performance performance = new Performance();
    private Tuning tuning = new Tuning();
    private Maintenance maintenance = new Maintenance();

    public static class Storage {
        private String dbPath = "rocksdb";
        private long maxStorageSize = 1024L * 1024L * 1024L; // 1GB
        private boolean compression = true;
        private String compressionType = "LZ4";
        private boolean enableWAL = true;

        // Getters and setters
        public String getDbPath() { return dbPath; }
        public void setDbPath(String dbPath) { this.dbPath = dbPath; }
        public long getMaxStorageSize() { return maxStorageSize; }
        public void setMaxStorageSize(long maxStorageSize) { this.maxStorageSize = maxStorageSize; }
        public boolean isCompression() { return compression; }
        public void setCompression(boolean compression) { this.compression = compression; }
        public String getCompressionType() { return compressionType; }
        public void setCompressionType(String compressionType) { this.compressionType = compressionType; }
        public boolean isEnableWAL() { return enableWAL; }
        public void setEnableWAL(boolean enableWAL) { this.enableWAL = enableWAL; }
    }

    public static class Performance {
        private long writeBufferSize = 64 * 1024 * 1024L; // 64MB
        private int maxWriteBufferNumber = 3;
        private int maxBackgroundCompactions = 4;
        private long blockCacheSize = 32 * 1024 * 1024L; // 32MB
        private int blockSize = 16 * 1024; // 16KB
        private boolean verifyChecksums = true;

        // Getters and setters
        public long getWriteBufferSize() { return writeBufferSize; }
        public void setWriteBufferSize(long writeBufferSize) { this.writeBufferSize = writeBufferSize; }
        public int getMaxWriteBufferNumber() { return maxWriteBufferNumber; }
        public void setMaxWriteBufferNumber(int maxWriteBufferNumber) { this.maxWriteBufferNumber = maxWriteBufferNumber; }
        public int getMaxBackgroundCompactions() { return maxBackgroundCompactions; }
        public void setMaxBackgroundCompactions(int maxBackgroundCompactions) { this.maxBackgroundCompactions = maxBackgroundCompactions; }
        public long getBlockCacheSize() { return blockCacheSize; }
        public void setBlockCacheSize(long blockCacheSize) { this.blockCacheSize = blockCacheSize; }
        public int getBlockSize() { return blockSize; }
        public void setBlockSize(int blockSize) { this.blockSize = blockSize; }
        public boolean isVerifyChecksums() { return verifyChecksums; }
        public void setVerifyChecksums(boolean verifyChecksums) { this.verifyChecksums = verifyChecksums; }
    }

    public static class Tuning {
        private long targetFileSizeBase = 64 * 1024 * 1024L; // 64MB
        private long maxBytesForLevelBase = 256 * 1024 * 1024L; // 256MB
        private int numLevels = 7;
        private double levelMultiplier = 10;

        // Getters and setters
        public long getTargetFileSizeBase() { return targetFileSizeBase; }
        public void setTargetFileSizeBase(long targetFileSizeBase) { this.targetFileSizeBase = targetFileSizeBase; }
        public long getMaxBytesForLevelBase() { return maxBytesForLevelBase; }
        public void setMaxBytesForLevelBase(long maxBytesForLevelBase) { this.maxBytesForLevelBase = maxBytesForLevelBase; }
        public int getNumLevels() { return numLevels; }
        public void setNumLevels(int numLevels) { this.numLevels = numLevels; }
        public double getLevelMultiplier() { return levelMultiplier; }
        public void setLevelMultiplier(double levelMultiplier) { this.levelMultiplier = levelMultiplier; }
    }

    public static class Maintenance {
        private int cleanupIntervalHours = 1;
        private int cleanupBatchSize = 1000;
        private boolean autoCompaction = true;
        private int maxCompactionThreads = 2;

        // Getters and setters
        public int getCleanupIntervalHours() { return cleanupIntervalHours; }
        public void setCleanupIntervalHours(int cleanupIntervalHours) { this.cleanupIntervalHours = cleanupIntervalHours; }
        public int getCleanupBatchSize() { return cleanupBatchSize; }
        public void setCleanupBatchSize(int cleanupBatchSize) { this.cleanupBatchSize = cleanupBatchSize; }
        public boolean isAutoCompaction() { return autoCompaction; }
        public void setAutoCompaction(boolean autoCompaction) { this.autoCompaction = autoCompaction; }
        public int getMaxCompactionThreads() { return maxCompactionThreads; }
        public void setMaxCompactionThreads(int maxCompactionThreads) { this.maxCompactionThreads = maxCompactionThreads; }
    }

    // Getters
    public Storage getStorage() { return storage; }
    public Performance getPerformance() { return performance; }
    public Tuning getTuning() { return tuning; }
    public Maintenance getMaintenance() { return maintenance; }
}

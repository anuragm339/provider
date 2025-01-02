package com.example.messaging.storage.db.rocks.model;

public record RocksStats(
        long totalKeys,
        long totalSize,
        long pendingCompactions,
        long memTableSize,
        long blockCacheUsage
) {
    public double getMemTableUtilization() {
        return memTableSize / (double)(64 * 1024 * 1024); // Relative to 64MB default
    }

    public double getBlockCacheHitRate() {
        return blockCacheUsage / (double)(32 * 1024 * 1024); // Relative to 32MB default
    }
}

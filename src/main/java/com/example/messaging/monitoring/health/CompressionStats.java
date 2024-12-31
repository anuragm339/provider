package com.example.messaging.monitoring.health;

public class CompressionStats {
    private final long totalMessages;
    private final double averageCompressionTimeMs;
    private final double averageOriginalSizeBytes;
    private final double compressionRatio;
    private final double totalSpaceSaved;

    public CompressionStats(long totalMessages,
                            double averageCompressionTimeMs,
                            double averageOriginalSizeBytes,
                            double compressionRatio) {
        this.totalMessages = totalMessages;
        this.averageCompressionTimeMs = averageCompressionTimeMs;
        this.averageOriginalSizeBytes = averageOriginalSizeBytes;
        this.compressionRatio = compressionRatio;
        this.totalSpaceSaved = calculateSpaceSaved();
    }

    private double calculateSpaceSaved() {
        return totalMessages * averageOriginalSizeBytes * (1 - compressionRatio);
    }

    public long getTotalMessages() {
        return totalMessages;
    }

    public double getAverageCompressionTimeMs() {
        return averageCompressionTimeMs;
    }

    public double getAverageOriginalSizeBytes() {
        return averageOriginalSizeBytes;
    }

    public double getCompressionRatio() {
        return compressionRatio;
    }

    public double getTotalSpaceSaved() {
        return totalSpaceSaved;
    }
}

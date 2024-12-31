package com.example.messaging.storage.model;

public class DatabaseHealthStatus {
    private final boolean isHealthy;
    private final long diskSpaceUsage;
    private final boolean canWrite;
    private final long queryResponseTime;
    private final long lastCheckTime;

    private DatabaseHealthStatus(Builder builder) {
        this.isHealthy = builder.isHealthy;
        this.diskSpaceUsage = builder.diskSpaceUsage;
        this.canWrite = builder.canWrite;
        this.queryResponseTime = builder.queryResponseTime;
        this.lastCheckTime = builder.lastCheckTime;
    }

    // Getters
    public boolean isHealthy() { return isHealthy; }
    public long getDiskSpaceUsage() { return diskSpaceUsage; }
    public boolean canWrite() { return canWrite; }
    public long getQueryResponseTime() { return queryResponseTime; }
    public long getLastCheckTime() { return lastCheckTime; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean isHealthy;
        private long diskSpaceUsage;
        private boolean canWrite;
        private long queryResponseTime;
        private long lastCheckTime;

        public Builder isHealthy(boolean isHealthy) {
            this.isHealthy = isHealthy;
            return this;
        }

        public Builder diskSpaceUsage(long diskSpaceUsage) {
            this.diskSpaceUsage = diskSpaceUsage;
            return this;
        }

        public Builder canWrite(boolean canWrite) {
            this.canWrite = canWrite;
            return this;
        }

        public Builder queryResponseTime(long queryResponseTime) {
            this.queryResponseTime = queryResponseTime;
            return this;
        }

        public Builder lastCheckTime(long lastCheckTime) {
            this.lastCheckTime = lastCheckTime;
            return this;
        }

        public DatabaseHealthStatus build() {
            return new DatabaseHealthStatus(this);
        }
    }
}

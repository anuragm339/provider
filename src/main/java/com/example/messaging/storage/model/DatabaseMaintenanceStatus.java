package com.example.messaging.storage.model;

public class DatabaseMaintenanceStatus {
    private final long lastMaintenanceTime;
    private final long compactionTime;
    private final long integrityCheckTime;
    private final long indexMaintenanceTime;
    private final boolean maintenanceInProgress;

    private DatabaseMaintenanceStatus(Builder builder) {
        this.lastMaintenanceTime = builder.lastMaintenanceTime;
        this.compactionTime = builder.compactionTime;
        this.integrityCheckTime = builder.integrityCheckTime;
        this.indexMaintenanceTime = builder.indexMaintenanceTime;
        this.maintenanceInProgress = builder.maintenanceInProgress;
    }

    public long getLastMaintenanceTime() { return lastMaintenanceTime; }
    public long getCompactionTime() { return compactionTime; }
    public long getIntegrityCheckTime() { return integrityCheckTime; }
    public long getIndexMaintenanceTime() { return indexMaintenanceTime; }
    public boolean isMaintenanceInProgress() { return maintenanceInProgress; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long lastMaintenanceTime;
        private long compactionTime;
        private long integrityCheckTime;
        private long indexMaintenanceTime;
        private boolean maintenanceInProgress;

        public Builder lastMaintenanceTime(long lastMaintenanceTime) {
            this.lastMaintenanceTime = lastMaintenanceTime;
            return this;
        }

        public Builder compactionTime(long compactionTime) {
            this.compactionTime = compactionTime;
            return this;
        }

        public Builder integrityCheckTime(long integrityCheckTime) {
            this.integrityCheckTime = integrityCheckTime;
            return this;
        }

        public Builder indexMaintenanceTime(long indexMaintenanceTime) {
            this.indexMaintenanceTime = indexMaintenanceTime;
            return this;
        }

        public Builder maintenanceInProgress(boolean maintenanceInProgress) {
            this.maintenanceInProgress = maintenanceInProgress;
            return this;
        }

        public DatabaseMaintenanceStatus build() {
            return new DatabaseMaintenanceStatus(this);
        }
    }
}

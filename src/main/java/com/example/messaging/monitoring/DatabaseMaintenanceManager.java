package com.example.messaging.monitoring;

import com.example.messaging.storage.db.sqlite.SQLiteConfig;
import com.example.messaging.storage.model.DatabaseMaintenanceStatus;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseMaintenanceManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseMaintenanceManager.class);

    private final DataSource dataSource;
    private final SQLiteConfig config;
    private final ScheduledExecutorService scheduler;

    private final AtomicLong lastMaintenanceTime = new AtomicLong(0);
    private final AtomicLong lastCompactionTime = new AtomicLong(0);
    private final AtomicLong lastIntegrityCheckTime = new AtomicLong(0);
    private final AtomicLong lastIndexMaintenanceTime = new AtomicLong(0);
    private final AtomicBoolean maintenanceInProgress = new AtomicBoolean(false);

    public DatabaseMaintenanceManager(DataSource dataSource, SQLiteConfig config) {
        this.dataSource = dataSource;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void startMaintenanceSchedule() {
        scheduler.scheduleAtFixedRate(
                this::performMaintenance,
                1, // initial delay
                24, // period
                TimeUnit.HOURS
        );
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void performMaintenance() {
        if (!maintenanceInProgress.compareAndSet(false, true)) {
            logger.warn("Maintenance already in progress, skipping...");
            return;
        }

        try {
            long startTime = System.currentTimeMillis();

            compactDatabase();
            checkDatabaseIntegrity();
            maintainIndexes();

            lastMaintenanceTime.set(System.currentTimeMillis() - startTime);
            logger.debug("Database maintenance completed successfully");
        } catch (Exception e) {
            logger.error("Database maintenance failed", e);
        } finally {
            maintenanceInProgress.set(false);
        }
    }

    public void compactDatabase() {
        long startTime = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Clean up any orphaned records
            stmt.execute("""
                DELETE FROM processing_results 
                WHERE msg_offset NOT IN (SELECT msg_offset FROM messages)
            """);

            // Vacuum the database
            stmt.execute("VACUUM");

            lastCompactionTime.set(System.currentTimeMillis() - startTime);
            logger.debug("Database compaction completed in {}ms", lastCompactionTime.get());
        } catch (SQLException e) {
            logger.error("Database compaction failed", e);
            throw new ProcessingException(
                    "Failed to compact database",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public void checkDatabaseIntegrity() {
        long startTime = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("PRAGMA integrity_check")) {
                if (rs.next()) {
                    String result = rs.getString(1);
                    if (!"ok".equalsIgnoreCase(result)) {
                        logger.error("Database integrity check failed: {}", result);
                        throw new ProcessingException(
                                "Database integrity check failed: " + result,
                                ErrorCode.PROCESSING_FAILED.getCode(),
                                false,
                                null
                        );
                    }
                }
            }

            lastIntegrityCheckTime.set(System.currentTimeMillis() - startTime);
            logger.debug("Database integrity check passed in {}ms", lastIntegrityCheckTime.get());
        } catch (SQLException e) {
            logger.error("Database integrity check failed", e);
            throw new ProcessingException(
                    "Failed to check database integrity",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public void maintainIndexes() {
        long startTime = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Get list of indexes
            List<String> indexes = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT name FROM sqlite_master WHERE type='index'")) {
                while (rs.next()) {
                    indexes.add(rs.getString(1));
                }
            }

            // Reindex each index
            for (String index : indexes) {
                stmt.execute("REINDEX " + index);
            }

            // Analyze for query optimization
            stmt.execute("ANALYZE");

            lastIndexMaintenanceTime.set(System.currentTimeMillis() - startTime);
            logger.debug("Index maintenance completed for {} indexes in {}ms",
                    indexes.size(), lastIndexMaintenanceTime.get());
        } catch (SQLException e) {
            logger.error("Index maintenance failed", e);
            throw new ProcessingException(
                    "Failed to maintain indexes",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public DatabaseMaintenanceStatus getMaintenanceStatus() {
        return DatabaseMaintenanceStatus.builder()
                .lastMaintenanceTime(lastMaintenanceTime.get())
                .compactionTime(lastCompactionTime.get())
                .integrityCheckTime(lastIntegrityCheckTime.get())
                .indexMaintenanceTime(lastIndexMaintenanceTime.get())
                .maintenanceInProgress(maintenanceInProgress.get())
                .build();
    }
}

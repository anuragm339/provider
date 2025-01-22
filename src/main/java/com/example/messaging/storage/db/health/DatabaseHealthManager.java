package com.example.messaging.storage.db.health;

import com.example.messaging.storage.db.sqlite.SQLiteConfig;
import com.example.messaging.storage.model.DatabaseHealthStatus;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseHealthManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseHealthManager.class);

    private final DataSource dataSource;
    private final SQLiteConfig config;
    private final AtomicLong lastCheckTime = new AtomicLong(0);
    private final AtomicLong lastQueryTime = new AtomicLong(0);
    private final AtomicLong diskSpaceUsage = new AtomicLong(0);

    public DatabaseHealthManager(DataSource dataSource, SQLiteConfig config) {
        this.dataSource = dataSource;
        this.config = config;
    }

    public boolean checkDatabaseHealth() {
        try {
            long startTime = System.currentTimeMillis();

            // Check connection
            if (!checkConnection()) {
                return false;
            }

            // Check disk space
            if (!checkDiskSpace()) {
                return false;
            }

            // Check write capability
            if (!checkWriteCapability()) {
                return false;
            }

            lastCheckTime.set(System.currentTimeMillis() - startTime);
            return true;
        } catch (Exception e) {
            logger.error("Database health check failed", e);
            return false;
        }
    }

    private boolean checkConnection() {
        try (Connection conn = dataSource.getConnection()) {
            return conn.isValid(5); // 5 seconds timeout
        } catch (SQLException e) {
            logger.error("Database connection check failed", e);
            return false;
        }
    }

    private long updateAndGetDiskSpaceUsage() {
        File dbFile = new File(config.getStoragePath());
        diskSpaceUsage.set(dbFile.length());
        return diskSpaceUsage.get();
    }

    private boolean checkDiskSpace() {
        File dbFile = new File(config.getStoragePath());
        File parentDir = dbFile.getParentFile();

        long usableSpace = parentDir.getUsableSpace();
        long totalSpace = parentDir.getTotalSpace();

        // Alert if less than 10% space available
        if (usableSpace < (totalSpace * 0.1)) {
            logger.warn("Low disk space: {}% available", (usableSpace * 100.0) / totalSpace);
            return false;
        }

        return true;
    }

    private boolean checkWriteCapability() {
        int retries = 0;
        int maxRetries = 30;
        long retryDelayMs = 1000;

        while (retries < maxRetries) {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS health_check (id INTEGER PRIMARY KEY)");
                    stmt.execute("INSERT INTO health_check (id) VALUES (1)");
                    stmt.execute("DELETE FROM health_check WHERE id = 1");
                    conn.commit();
                    return true;
                } catch (SQLException e) {
                    conn.rollback();
                    if (e.getMessage().contains("database is locked")) {
                        retries++;
                        if (retries < maxRetries) {
                            logger.warn("Database locked, retrying in {} ms. Attempt {}/{}",
                                    retryDelayMs, retries, maxRetries);
                            Thread.sleep(retryDelayMs);
                            continue;
                        }
                    }
                    throw e;
                }
            } catch (Exception e) {
                logger.error("Write capability check failed", e);
                return false;
            }
        }
        return false;
    }

    private long checkQueryResponseTime() {
        long startTime = System.currentTimeMillis();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM messages")) {
                rs.next();
            }

            long responseTime = System.currentTimeMillis() - startTime;
            lastQueryTime.set(responseTime);
            return responseTime;
        } catch (SQLException e) {
            logger.error("Query response time check failed", e);
            throw new ProcessingException(
                    "Failed to check query response time",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public DatabaseHealthStatus getHealthStatus() {
        boolean isHealthy = checkDatabaseHealth();
        long currentQueryTime = checkQueryResponseTime();
        long currentDiskSpace = updateAndGetDiskSpaceUsage();

        return DatabaseHealthStatus.builder()
                .isHealthy(isHealthy)
                .diskSpaceUsage(currentDiskSpace)
                .canWrite(checkWriteCapability())
                .queryResponseTime(currentQueryTime)
                .lastCheckTime(lastCheckTime.get())
                .build();
    }
}

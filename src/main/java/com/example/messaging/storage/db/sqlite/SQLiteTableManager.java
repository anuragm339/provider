package com.example.messaging.storage.db.sqlite;

import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLiteTableManager {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTableManager.class);

    private static final String CREATE_MESSAGES_TABLE = """
        CREATE TABLE IF NOT EXISTS messages (
            msg_offset       BIGINT PRIMARY KEY,
            msg_key          text,
            type            VARCHAR(50) NOT NULL,
            created_utc     TIMESTAMP NOT NULL,
            data            BLOB NOT NULL,
            state           text DEFAULT 'PENDING'
        )
    """;

    private static final String CREATE_PROCESSING_RESULTS_TABLE = """
        CREATE TABLE IF NOT EXISTS processing_results (
            msg_offset          BIGINT NOT NULL,
            success            BOOLEAN NOT NULL,
            checksum           VARCHAR(64) NOT NULL,
            processing_time    BIGINT NOT NULL,
            error_code         VARCHAR(10),
            error_message      TEXT
        )
    """;

    private static final String CREATE_INDEXES = """
        CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(type);
        CREATE INDEX IF NOT EXISTS idx_messages_created_utc ON messages(created_utc);
        CREATE INDEX IF NOT EXISTS idx_results_processing_time ON processing_results(processing_time);
    """;

    private final DataSource dataSource;

    public SQLiteTableManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void initializeTables() {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_MESSAGES_TABLE);
                stmt.execute(CREATE_PROCESSING_RESULTS_TABLE);

                // Create indexes
                for (String indexSql : CREATE_INDEXES.split(";")) {
                    if (!indexSql.trim().isEmpty()) {
                        stmt.execute(indexSql);
                    }
                }

                conn.commit();
                logger.debug("Database tables initialized successfully");
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            logger.error("Failed to initialize database tables", e);
            throw new ProcessingException(
                    "Database initialization failed",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    public void validateSchema() {
        try (Connection conn = dataSource.getConnection()) {
            // Check messages table structure
            conn.prepareStatement("""
                SELECT msg_offset, type, created_utc, data, 
                       state, msg_key 
                FROM messages WHERE 1=0
                """).executeQuery();

            // Check processing_results table structure
            conn.prepareStatement("""
                SELECT msg_offset, success, checksum, processing_time, 
                       error_code, error_message 
                FROM processing_results WHERE 1=0
                """).executeQuery();

            logger.debug("Database schema validation successful");
        } catch (SQLException e) {
            logger.error("Database schema validation failed", e);
            throw new ProcessingException(
                    "Invalid database schema",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    false,
                    e
            );
        }
    }

    public void cleanup(long retentionPeriodMs) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Delete old messages and their results
                try (Statement stmt = conn.createStatement()) {
                    String deleteSql = String.format("""
                        DELETE FROM processing_results 
                        WHERE msg_offset IN (
                            SELECT msg_offset FROM messages 
                            WHERE created_utc < datetime('now', '-%d seconds')
                        )
                        """, retentionPeriodMs / 1000);
                    stmt.executeUpdate(deleteSql);

                    deleteSql = String.format("""
                        DELETE FROM messages 
                        WHERE created_utc < datetime('now', '-%d seconds')
                        """, retentionPeriodMs / 1000);
                    stmt.executeUpdate(deleteSql);
                }

                // Vacuum to reclaim space
                if (conn.getMetaData().supportsBatchUpdates()) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("VACUUM");
                    }
                }

                conn.commit();
                logger.debug("Database cleanup completed successfully");
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            logger.error("Database cleanup failed", e);
            throw new ProcessingException(
                    "Database cleanup failed",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }
}

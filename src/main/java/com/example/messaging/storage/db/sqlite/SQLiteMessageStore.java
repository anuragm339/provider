package com.example.messaging.storage.db.sqlite;

import com.example.messaging.core.pipeline.impl.DefaultProcessingResult;
import com.example.messaging.models.MessageState;
import com.example.messaging.monitoring.alerts.PerformanceAlert;
import com.example.messaging.monitoring.health.CompressionStats;
import com.example.messaging.storage.db.health.DatabaseHealthManager;
import com.example.messaging.monitoring.DatabaseMaintenanceManager;
import com.example.messaging.core.compression.MessageCompression;
import com.example.messaging.monitoring.metrics.MessageStatistics;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.storage.model.*;
import com.example.messaging.models.Message;
import com.example.messaging.core.pipeline.service.ProcessingResult;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Singleton
public class SQLiteMessageStore implements MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteMessageStore.class);

    private static final String INSERT_MESSAGE =
            "INSERT INTO messages (msg_offset, msg_key,type, created_utc, data) " +
                    "VALUES (?, ?,?, ?, ? )";

    private static final String SELECT_MESSAGE =
            "SELECT msg_offset, type, created_utc, data, compressed FROM messages WHERE msg_offset = ?";

    private static final String INSERT_RESULT =
            "INSERT INTO processing_results (msg_offset, success, checksum, processing_time) VALUES (?, ?, ?, ?)";

    private static final String SELECT_RESULT =
            "SELECT msg_offset, success, checksum, processing_time FROM processing_results WHERE msg_offset = ?";

    private static final String COUNT_MESSAGES = "SELECT COUNT(*) FROM messages";

    private static final String UPDATE_PROCESSING_RESULT = "UPDATE processing_results SET success = ? WHERE msg_offset  IN (%s)";

    private static final String UPDATE_MSG_STATE = "UPDATE messages SET state = ? WHERE msg_offset IN (%s);";

    @Value("${message.store.batch-size:10}")
    private int defaultBatchSize;

    private static final String SELECT_MESSAGES_AFTER_OFFSET =
            "SELECT * FROM messages WHERE msg_offset > ? and type= ? and state != 'DELIVERED' ORDER BY msg_offset ASC LIMIT ?";

    private final DataSource dataSource;
    private final SQLiteConfig config;
    private final Executor executor;
    private final SQLiteTableManager tableManager;
    private final DatabaseHealthManager healthManager;
    private final DatabaseMaintenanceManager maintenanceManager;
    private final MessageCompression compression;
    private final MessageStatistics statistics;

    public SQLiteMessageStore(DataSource dataSource, SQLiteConfig config, Executor executor,MessageCompression messageCompression) {
        this.dataSource = dataSource;
        this.config = config;
        this.executor = executor;
        this.tableManager = new SQLiteTableManager(dataSource);
        this.healthManager = new DatabaseHealthManager(dataSource, config);
        this.maintenanceManager = new DatabaseMaintenanceManager(dataSource, config);
        this.compression = messageCompression;
        this.statistics = new MessageStatistics(dataSource);

        initialize();
    }

    private void initialize() {
        tableManager.initializeTables();
        tableManager.validateSchema();
        maintenanceManager.startMaintenanceSchedule();
    }

    @Override
    public CompletableFuture<Long> store(Message message) {
        long startTime = System.currentTimeMillis();
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();

                 PreparedStatement stmt = conn.prepareStatement(INSERT_MESSAGE)) {

                byte[] dataToStore = message.getData();
//                boolean isCompressed = false;
//                int originalSize = dataToStore.length;
//                int compressedSize = originalSize;

//                if (compression.shouldCompress(message)) {
//                    dataToStore = compression.compressData(message.getData());
//                    isCompressed = true;
//                    compressedSize = dataToStore.length;
//                }

                stmt.setLong(1, message.getMsgOffset());
                stmt.setString(2,message.getMsgKey());
                stmt.setString(3, message.getType());
                stmt.setTimestamp(4, Timestamp.from(message.getCreatedUtc()));
                stmt.setBytes(5, dataToStore);
                stmt.executeUpdate();

                PreparedStatement preparedStatement = conn.prepareStatement(COUNT_MESSAGES);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    logger.info("Number of messages in the database: {}", resultSet.getInt(1));
                }

                long duration = System.currentTimeMillis() - startTime;
                statistics.recordWriteTime(message.getType(), duration);

                return 0l;
            } catch (SQLException e) {
                logger.error("Failed to store message: {}", message.getMsgOffset(), e);
                throw new ProcessingException(
                        "Failed to store message",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public CompletableFuture<List<Long>> storeBatch(List<Message> messages) {
        if (!canAccept()) {
            return CompletableFuture.failedFuture(
                    new ProcessingException(
                            "Store cannot accept more messages",
                            ErrorCode.QUEUE_FULL.getCode(),
                            true,
                            null
                    )
            );
        }

        long startTime = System.currentTimeMillis();
        return CompletableFuture.supplyAsync(() -> {
            List<Long> offsets = new ArrayList<>();
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement stmt = conn.prepareStatement(INSERT_MESSAGE)) {
                    for (Message message : messages) {
                        byte[] dataToStore = message.getData();
                        boolean isCompressed = false;
                        int originalSize = dataToStore.length;
                        int compressedSize = originalSize;

                        if (compression.shouldCompress(message)) {
                            dataToStore = compression.compressData(message.getData());
                            isCompressed = true;
                            compressedSize = dataToStore.length;
                        }

                        stmt.setLong(1, message.getMsgOffset());
                        stmt.setString(2,message.getMsgKey());
                        stmt.setString(3, message.getType());
                        stmt.setTimestamp(4, Timestamp.from(message.getCreatedUtc()));
                        stmt.setBytes(5, dataToStore);
                        stmt.setBoolean(6, isCompressed);
                        stmt.setInt(7, originalSize);
                        stmt.setInt(8, compressedSize);

                        stmt.addBatch();
                        offsets.add(message.getMsgOffset());
                    }

                    stmt.executeBatch();
                    conn.commit();

                    long duration = (System.currentTimeMillis() - startTime) / messages.size();
                    messages.forEach(msg -> statistics.recordWriteTime(msg.getType(), duration));

                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            } catch (SQLException e) {
                logger.error("Failed to store batch of messages", e);
                throw new ProcessingException(
                        "Failed to store message batch",
                        ErrorCode.BATCH_PROCESSING_ERROR.getCode(),
                        true,
                        e
                );
            }
            return offsets;
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<Message>> getMessage(long offset) {
        long startTime = System.currentTimeMillis();
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(SELECT_MESSAGE)) {

                stmt.setLong(1, offset);
                ResultSet rs = stmt.executeQuery();

                if (rs.next()) {
                    byte[] data = rs.getBytes("data");
                    boolean isCompressed = rs.getBoolean("compressed");

                    if (isCompressed) {
                        data = compression.decompressData(data);
                    }

                    Message message = Message.builder()
                            .msgOffset(rs.getLong("msg_offset"))
                            .type(rs.getString("type"))
                            .createdUtc(rs.getTimestamp("created_utc").toInstant())
                            .data(data)
                            .build();

                    long duration = System.currentTimeMillis() - startTime;
                    statistics.recordReadTime(message.getType(), duration);

                    return Optional.of(message);
                }

                return Optional.empty();
            } catch (SQLException e) {
                logger.error("Failed to retrieve message: {}", offset, e);
                throw new ProcessingException(
                        "Failed to retrieve message",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> storeProcessingResult(ProcessingResult result) {
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(INSERT_RESULT)) {

                stmt.setLong(1, result.getOffset());
                stmt.setBoolean(2, result.isSuccessful());
                stmt.setString(3, result.getChecksum());
                stmt.setLong(4, result.getProcessingTimestamp());

                stmt.executeUpdate();
            } catch (SQLException e) {
                logger.error("Failed to store processing result: {}", result.getOffset(), e);
                throw new ProcessingException(
                        "Failed to store processing result",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<ProcessingResult>> getProcessingResult(long offset) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(SELECT_RESULT)) {

                stmt.setLong(1, offset);
                ResultSet rs = stmt.executeQuery();

                if (rs.next()) {
                    ProcessingResult result = new DefaultProcessingResult.Builder()
                            .offset(rs.getLong("msg_offset"))
                            .successful(rs.getBoolean("success"))
                            .checksum(rs.getString("checksum"))
                            .processingTimestamp(rs.getLong("processing_time"))
                            .build();
                    return Optional.of(result);
                }

                return Optional.empty();
            } catch (SQLException e) {
                logger.error("Failed to retrieve processing result: {}", offset, e);
                throw new ProcessingException(
                        "Failed to retrieve processing result",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public boolean canAccept() {
        return healthManager.checkDatabaseHealth() &&
                getDatabaseHealthStatus().isHealthy();
    }

    @Override
    public boolean isHealthy() {
        return getDatabaseHealthStatus().isHealthy();
    }

    public DatabaseHealthStatus getDatabaseHealthStatus() {
        return healthManager.getHealthStatus();
    }

    public DatabaseMaintenanceStatus getMaintenanceStatus() {
        return maintenanceManager.getMaintenanceStatus();
    }

    public CompressionStats getCompressionStats() {
        return compression.getCompressionStats();
    }

    public MessageStats getMessageStats() {
        return statistics.getStatistics();
    }

    public List<PerformanceAlert> checkPerformance() {
        return statistics.checkPerformance();
    }

    public void shutdown() {
        maintenanceManager.shutdown();
    }
    @Override
    public CompletableFuture<Integer> deleteMessagesBeforeOffset(long offset) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "DELETE FROM messages WHERE msg_offset < ?")) {

                stmt.setLong(1, offset);
                return stmt.executeUpdate();
            } catch (SQLException e) {
                logger.error("Failed to delete messages before offset: {}", offset, e);
                throw new ProcessingException(
                        "Failed to delete old messages",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Long> getCurrentOffset() {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                         "SELECT MAX(msg_offset) FROM messages")) {

                ResultSet rs = stmt.executeQuery();
                return rs.next() ? rs.getLong(1) : 0L;
            } catch (SQLException e) {
                logger.error("Failed to get current offset", e);
                throw new ProcessingException(
                        "Failed to get current offset",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Integer> deleteMessagesWithOffsets(Set<Long> offsets) {
        logger.info("Deleting messages with offsets: {}", offsets);
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    // First delete from processing_results (dependent table)
                    int deletedResultsCount = deleteFromProcessingResults(conn, offsets);
                    logger.debug("Deleted {} entries from processing_results", deletedResultsCount);

                    // Then delete from messages (main table)
                    int deletedMessagesCount = deleteFromMessages(conn, offsets);
                    logger.debug("Deleted {} messages", deletedMessagesCount);

                    conn.commit();
                    return deletedMessagesCount;

                } catch (SQLException e) {
                    conn.rollback();
                    logger.error("Failed to delete messages with offsets: {}", offsets, e);
                    throw new ProcessingException(
                            "Failed to delete messages",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true,
                            e
                    );
                }
            } catch (SQLException e) {
                throw new ProcessingException(
                        "Database connection failed during deletion",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    private int deleteFromProcessingResults(Connection conn, Set<Long> offsets)
            throws SQLException {
        String sql = "DELETE FROM processing_results WHERE msg_offset = ?";
        int deletedCount = 0;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Long offset : offsets) {
                stmt.setLong(1, offset);
                deletedCount += stmt.executeUpdate();
            }
        }
        return deletedCount;
    }

    private int deleteFromMessages(Connection conn, Set<Long> offsets)
            throws SQLException {
        String sql = "DELETE FROM messages WHERE msg_offset = ?";
        int deletedCount = 0;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Long offset : offsets) {
                stmt.setLong(1, offset);
                deletedCount += stmt.executeUpdate();
            }
        }
        return deletedCount;
    }

    // For better performance with large batches
    private int deleteFromProcessingResultsBatch(Connection conn, Set<Long> offsets)
            throws SQLException {
        String sql = "DELETE FROM processing_results WHERE msg_offset IN " +
                "(SELECT msg_offset FROM messages WHERE msg_offset = ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int batchSize = 0;
            for (Long offset : offsets) {
                stmt.setLong(1, offset);
                stmt.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    stmt.executeBatch();
                    batchSize = 0;
                }
            }
            if (batchSize > 0) {
                stmt.executeBatch();
            }
        }
        return offsets.size(); // Approximate count
    }
    @Override
    public CompletableFuture<Void> storeProcessingResultBatch(List<ProcessingResult> results) {
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try (PreparedStatement stmt = conn.prepareStatement(INSERT_RESULT)) {
                    for (ProcessingResult result : results) {
                        stmt.setLong(1, result.getOffset());
                        stmt.setBoolean(2, result.isSuccessful());
                        stmt.setString(3, result.getChecksum());
                        stmt.setLong(4, result.getProcessingTimestamp());
                        stmt.addBatch();
                    }

                    stmt.executeBatch();
                    conn.commit();

                    logger.info("Successfully stored {} processing results", results.size());
                } catch (SQLException e) {
                    conn.rollback();
                    logger.error("Failed to store processing results batch", e);
                    throw new ProcessingException(
                            "Failed to store processing results",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true,
                            e
                    );
                }
            } catch (SQLException e) {
                logger.error("Database error while storing processing results", e);
                throw new ProcessingException(
                        "Database error",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }

    @Override
    public List<Message> getMessagesAfterOffset(long offset,String type) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_MESSAGES_AFTER_OFFSET)) {

            stmt.setLong(1, offset);
            stmt.setString(2,type);
            stmt.setInt(3, defaultBatchSize);

            List<Message> messages = new ArrayList<>();

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Message message = Message.builder()
                            .msgOffset(rs.getLong("msg_offset"))
                            .type(rs.getString("type"))
                            .createdUtc(rs.getTimestamp("created_utc").toInstant())
                            .data(rs.getBytes("data"))
                            .build();

                    messages.add(message);
                }
            }

            return messages;
        } catch (SQLException e) {
            logger.error("Failed to retrieve messages after offset", e);
            throw new CompletionException(
                    new ProcessingException(
                            "Failed to retrieve messages",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true,
                            e
                    )
            );
        }
    }
    @Override
    public CompletableFuture<List<Message>> getMessagesByOffsets(List<Long> offsets) {
        return CompletableFuture.supplyAsync(() -> {
            List<Message> messages = new ArrayList<>();

            try (Connection conn = dataSource.getConnection()) {
                // Create parameterized query with correct number of placeholders
                String placeholders = String.join(",", Collections.nCopies(offsets.size(), "?"));
                String sql = "SELECT msg_offset, msg_key, type, created_utc, data " +
                        "FROM messages WHERE msg_offset IN (" + placeholders + ") " +
                        "ORDER BY msg_offset ASC";

                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    // Set parameters
                    for (int i = 0; i < offsets.size(); i++) {
                        stmt.setLong(i + 1, offsets.get(i));
                    }

                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        byte[] data = rs.getBytes("data");

                        Message message = Message.builder()
                                .msgOffset(rs.getLong("msg_offset"))
                                .msgKey(rs.getString("msg_key"))
                                .type(rs.getString("type"))
                                .createdUtc(rs.getTimestamp("created_utc").toInstant())
                                .data(data)
                                .build();

                        messages.add(message);
                    }
                }
            } catch (SQLException e) {
                logger.error("Failed to retrieve messages by offsets", e);
                throw new ProcessingException(
                        "Failed to retrieve messages",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }

            // Verify we found all requested messages
            if (messages.size() != offsets.size()) {
                logger.warn("Not all requested messages were found. Requested: {}, Found: {}",
                        offsets.size(), messages.size());
            }

            return messages;
        }, executor);
    }

    @Override
    public CompletableFuture<Void> updateMessageStatus(List<Long> offsets, MessageState messageState, String consumerId) {
        return CompletableFuture.runAsync(() -> {
            if (offsets.isEmpty()) {
                return; // No offsets to process
            }

            // Generate placeholders for IN clause
            String placeholders = offsets.stream()
                    .map(o -> "?")
                    .collect(Collectors.joining(","));
            String updateQuery = String.format(UPDATE_MSG_STATE, placeholders);

            String processQuery=String.format(UPDATE_PROCESSING_RESULT,placeholders);

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);

                try (PreparedStatement stmt = conn.prepareStatement(updateQuery);) {
                    PreparedStatement statement=conn.prepareStatement(processQuery);
                    // Set the state parameter
                    stmt.setString(1, messageState.name());
                    statement.setBoolean(1, messageState == MessageState.DELIVERED);
                    // Set the offset parameters
                    for (int i = 0; i < offsets.size(); i++) {
                        stmt.setLong(i + 2, offsets.get(i)); // Start from index 2 since index 1 is the state
                        statement.setLong(i + 2, offsets.get(i));
                    }

                    // Execute the update
                    stmt.executeUpdate();
                    statement.executeUpdate();

                    // Commit the transaction
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    logger.error("Failed to update message states with IN clause", e);
                    throw new ProcessingException(
                            "Failed to update processing results",
                            ErrorCode.PROCESSING_FAILED.getCode(),
                            true,
                            e
                    );
                }
            } catch (SQLException e) {
                logger.error("Database connection error while updating message states", e);
                throw new ProcessingException(
                        "Database connection error",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            }
        }, executor);
    }


}

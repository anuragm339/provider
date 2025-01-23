package com.example.messaging.storage.db.sqlite;

import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.exceptions.ProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Context
public class ConsumerOffsetTracker {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetTracker.class);
    private static final String OFFSET_FILE_NAME = "consumer_offsets.json";

    // Concurrent map to store offsets
    private final Map<String, ConsumerOffset> consumerOffsets = new ConcurrentHashMap<>();

    // Object mapper for JSON serialization
    private final ObjectMapper objectMapper;

    // File path for persistent storage
    private final Path offsetStoragePath;

    private static final String DEFAULT_OFFSET_FILE_NAME = "consumer_offsets.json";


    // Lock for thread-safe file operations`1422`s3 3CC BFVYVFDZAFAA#4ZAE`VGCXZSAWQ
    private final ReentrantLock fileLock = new ReentrantLock();

    public ConsumerOffsetTracker(
            @Value("${message.offset.storage.path}") String configuredPath
    ) {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        // Determine storage path
        if (configuredPath != null && !configuredPath.trim().isEmpty()) {
            this.offsetStoragePath = Paths.get(configuredPath, DEFAULT_OFFSET_FILE_NAME);
        } else {
            // Fallback to default location
            String userHome = System.getProperty("user.home");
            this.offsetStoragePath = Paths.get(userHome, ".message-system", DEFAULT_OFFSET_FILE_NAME);
        }

        // Ensure directory exists
        try {
            Files.createDirectories(offsetStoragePath.getParent());
        } catch (IOException e) {
            logger.error("Failed to create offset storage directory", e);
            throw new ProcessingException(
                    "Failed to create offset storage directory",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    false,
                    e
            );
        }

        // Load existing offsets
        loadPersistedOffsets();
    }


    /**
     * Update offset for a specific consumer in a group
     *
     * @param consumerId Unique consumer identifier
     * @param groupId    Consumer group identifier
     * @param offset     New offset value
     */
    public void updateConsumerOffset(String consumerId, String groupId, long offset) {
        String key = generateKey(consumerId, groupId);

        // Update in-memory offset
        ConsumerOffset consumerOffset = new ConsumerOffset(offset, consumerId, groupId);
        consumerOffsets.put(key, consumerOffset);

        // Persist to file
        persistOffsets();
    }

    /**
     * Get the last processed offset for a specific group
     *
     * @param groupId Consumer group identifier
     * @return Last processed offset, or 0 if no offset exists
     */
    public long getLastProcessedOffset(String groupId) {
        return consumerOffsets.values().stream()
                .filter(offset -> offset.getGroupId().equals(groupId))
                .mapToLong(ConsumerOffset::getOffset)
                .max()
                .orElse(0L);
    }

    /**
     * Get the last processed offset for a specific consumer in a group
     *
     * @param consumerId Unique consumer identifier
     * @param groupId    Consumer group identifier
     * @return Last processed offset, or 0 if no offset exists
     */
    public long getLastProcessedOffset(String consumerId, String groupId) {
        String key = generateKey(consumerId, groupId);
        return consumerOffsets.getOrDefault(key, new ConsumerOffset(0L, consumerId, groupId)).getOffset();
    }

    /**
     * Reset offset for a specific consumer or group
     *
     * @param consumerId Unique consumer identifier
     * @param groupId    Consumer group identifier
     * @param newOffset  New offset value
     */
    public void resetOffset(String consumerId, String groupId, long newOffset) {
        updateConsumerOffset(consumerId, groupId, newOffset);
    }

    /**
     * Generate a unique key for consumer-group combination
     *
     * @param consumerId Unique consumer identifier
     * @param groupId    Consumer group identifier
     * @return Unique key string
     */
    private String generateKey(String consumerId, String groupId) {
        return consumerId + ":" + groupId;
    }

    /**
     * Persist offsets to file
     */
    private void persistOffsets() {
        fileLock.lock();
        try {
            // Convert to map for serialization
            Map<String, ConsumerOffset> offsetMap = new ConcurrentHashMap<>(consumerOffsets);

            // Write to file
            String jsonContent = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(offsetMap);

            Files.writeString(
                    offsetStoragePath,
                    jsonContent,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            logger.error("Failed to persist offsets", e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * Load persisted offsets from file
     */
    private void loadPersistedOffsets() {
        if (!Files.exists(offsetStoragePath)) {
            try {
                Files.createFile(offsetStoragePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        try {
            String jsonContent = Files.readString(offsetStoragePath);
            if(jsonContent==null || jsonContent.isEmpty()) {
                return;
            }
            // Read offsets from file
            Map<String, ConsumerOffset> loadedOffsets = objectMapper.readValue(
                    jsonContent,
                    objectMapper.getTypeFactory().constructMapType(
                            Map.class,
                            String.class,
                            ConsumerOffset.class
                    )
            );

            // Update in-memory offsets
            consumerOffsets.putAll(loadedOffsets);
        } catch (IOException e) {
            logger.error("Failed to load persisted offsets", e);
        }
    }

    /**
     * Get all current offsets
     *
     * @return Map of all current offsets
     */
    public Map<String, ConsumerOffset> getAllOffsets() {
        return new ConcurrentHashMap<>(consumerOffsets);
    }

    /**
     * Clear all offsets
     */
    public void clearOffsets() {
        consumerOffsets.clear();
        persistOffsets();
    }
}

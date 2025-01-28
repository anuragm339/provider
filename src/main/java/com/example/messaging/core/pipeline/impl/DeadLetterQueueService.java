package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.model.DeadLetterMessage;
import com.example.messaging.models.Message;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Singleton
public class DeadLetterQueueService {
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueService.class);
    private static final String DEFAULT_DLQ_FILE_NAME = "dead_letter_queue.json";

    // In-memory queue for dead letter messages
    private final ConcurrentLinkedQueue<DeadLetterMessage> deadLetterQueue = new ConcurrentLinkedQueue<>();

    // Object mapper for JSON serialization
    private final ObjectMapper objectMapper;

    // File path for persistent storage
    private final Path dlqStoragePath;

    // Lock for thread-safe file operations
    private final ReentrantLock fileLock = new ReentrantLock();

    // Configuration parameters
    private final int maxQueueSize;
    private final long maxMessageRetentionDays;

    public DeadLetterQueueService(
            @Value("${message.dlq.storage.path}") String configuredPath,
            @Value("${message.dlq.max-size:10000}") int maxQueueSize,
            @Value("${message.dlq.retention-days:30}") long maxMessageRetentionDays
    ) {
        this.objectMapper = new ObjectMapper();
        this.maxQueueSize = maxQueueSize;
        this.maxMessageRetentionDays = maxMessageRetentionDays;

        // Determine storage path
        if (configuredPath != null && !configuredPath.trim().isEmpty()) {
            this.dlqStoragePath = Paths.get(configuredPath, DEFAULT_DLQ_FILE_NAME);
        } else {
            // Fallback to default location
            String userHome = System.getProperty("user.home");
            this.dlqStoragePath = Paths.get(userHome, ".message-system", DEFAULT_DLQ_FILE_NAME);
        }

        // Ensure directory exists
        try {
            Files.createDirectories(dlqStoragePath.getParent());
        } catch (IOException e) {
            logger.error("Failed to create Dead Letter Queue storage directory", e);
            throw new ProcessingException(
                    "Failed to create Dead Letter Queue storage directory",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    false,
                    e
            );
        }

        // Load existing DLQ messages
        loadPersistedMessages();
    }



    /**
     * Enqueue a message to the Dead Letter Queue
     * @param originalMessage The original message that failed processing
     * @param errorReason Reason for failure
     */
    public void enqueue(Message originalMessage, String errorReason) {
        // Create Dead Letter Message
        DeadLetterMessage dlqMessage = new DeadLetterMessage(originalMessage, errorReason);

        // Manage queue size
        if (deadLetterQueue.size() >= maxQueueSize) {
            // Remove oldest message if queue is full
            deadLetterQueue.poll();
        }

        // Add to queue
        deadLetterQueue.offer(dlqMessage);

        // Persist immediately
        persistMessages();

        // Log the failure
        logger.error("Message added to Dead Letter Queue: {} - Reason: {}",
                originalMessage.getMsgOffset(), errorReason);
    }

    /**
     * Retrieve and remove the next message from the queue
     * @return Next Dead Letter Message, or null if queue is empty
     */
    public DeadLetterMessage dequeue() {
        DeadLetterMessage message = deadLetterQueue.poll();

        if (message != null) {
            persistMessages();
        }

        return message;
    }

    /**
     * Get all current Dead Letter Queue messages
     * @return List of all messages in the queue
     */
    public List<DeadLetterMessage> getAllMessages() {
        return new ArrayList<>(deadLetterQueue);
    }

    /**
     * Get messages older than the retention period
     * @return List of expired messages
     */
    public List<DeadLetterMessage> getExpiredMessages() {
        Instant expirationTime = Instant.now().minusSeconds(maxMessageRetentionDays);
        return deadLetterQueue.stream()
                .filter(msg -> msg.getTimestamp().isBefore(expirationTime))
                .collect(Collectors.toList());
    }

    /**
     * Remove expired messages from the queue
     */
    public void removeExpiredMessages() {
        Instant expirationTime = Instant.now().minusSeconds(maxMessageRetentionDays);

        deadLetterQueue.removeIf(msg -> msg.getTimestamp().isBefore(expirationTime));

        persistMessages();

        logger.info("Removed expired messages from Dead Letter Queue");
    }

    /**
     * Persist messages to file
     */
    private void persistMessages() {
        fileLock.lock();
        try {
            // Convert to list for serialization
            List<DeadLetterMessage> messageList = new ArrayList<>(deadLetterQueue);

            // Write to file
            String jsonContent = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(messageList);

            Files.writeString(
                    dlqStoragePath,
                    jsonContent,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            logger.error("Failed to persist Dead Letter Queue messages", e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * Load persisted messages from file
     */
    private void loadPersistedMessages() {
        if (!Files.exists(dlqStoragePath)) {
            try {
                Files.createFile(dlqStoragePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        try {
            String jsonContent = Files.readString(dlqStoragePath);

            if(jsonContent==null || jsonContent.isEmpty()) {
                return;
            }
            // Read messages from file
            List<DeadLetterMessage> loadedMessages = objectMapper.readValue(
                    jsonContent,
                    objectMapper.getTypeFactory().constructCollectionType(
                            List.class,
                            DeadLetterMessage.class
                    )
            );

            // Clear existing queue and add loaded messages
            deadLetterQueue.clear();
            deadLetterQueue.addAll(loadedMessages);

            // Remove expired messages during load
            removeExpiredMessages();
        } catch (IOException e) {
            logger.error("Failed to load persisted Dead Letter Queue messages", e);
        }
    }

    /**
     * Clear all messages from the queue
     */
    public void clearQueue() {
        deadLetterQueue.clear();
        persistMessages();
        logger.info("Dead Letter Queue cleared");
    }

    /**
     * Get current queue size
     * @return Number of messages in the queue
     */
    public int getQueueSize() {
        return deadLetterQueue.size();
    }

    /**
     * Get the current storage path
     * @return Path to the Dead Letter Queue storage file
     */
    public Path getStoragePath() {
        return dlqStoragePath;
    }
}

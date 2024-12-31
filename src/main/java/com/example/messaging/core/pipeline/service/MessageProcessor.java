package com.example.messaging.core.pipeline.service;

import com.example.messaging.models.Message;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for processing messages with strong data integrity guarantees
 */
public interface MessageProcessor {

    /**
     * Processes a single message with integrity checks
     *
     * @param message The message to process
     * @return CompletableFuture containing processed message offset and integrity token
     */
    CompletableFuture<ProcessingResult> processMessage(Message message);

    /**
     * Verifies message was processed correctly
     *
     * @param messageOffset The message offset to verify
     * @param integrityToken The token received after processing
     * @return true if message processing was verified
     */
    boolean verifyProcessing(long messageOffset, String integrityToken);

    /**
     * Checks if processor can accept more messages
     */
    boolean canAccept();

    /**
     * Gets current processor status
     */
    boolean isOperational();


}

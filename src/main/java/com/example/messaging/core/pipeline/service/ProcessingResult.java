package com.example.messaging.core.pipeline.service;

/**
 * Result of message processing including integrity information
 */
public interface ProcessingResult {
    /**
     * Get the offset of the processed message
     */
    long getOffset();

    /**
     * Get the integrity token for verification
     */
    String getIntegrityToken();

    /**
     * Get the processing timestamp
     */
    long getProcessingTimestamp();

    /**
     * Check if processing was successful
     */
    boolean isSuccessful();

    /**
     * Get integrity check value
     */
    String getChecksum();
}

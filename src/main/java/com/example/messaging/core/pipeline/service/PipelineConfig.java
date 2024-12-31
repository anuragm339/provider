package com.example.messaging.core.pipeline.service;

/**
 * Configuration interface for pipeline settings
 */
public interface PipelineConfig {

    /**
     * Maximum number of messages that can be processed simultaneously
     */
    int getMaxConcurrentMessages();

    /**
     * Maximum size of the internal processing queue
     */
    int getMaxQueueSize();

    /**
     * Maximum time in milliseconds to wait for message processing
     */
    long getProcessingTimeoutMs();

    /**
     * Whether to enable retry for failed messages
     */
    boolean isRetryEnabled();

    /**
     * Maximum number of retry attempts for a message
     */
    int getMaxRetryAttempts();

    /**
     * Base delay in milliseconds between retries
     */
    long getRetryDelayMs();

    /**
     * Default implementation of pipeline configuration
     */

}

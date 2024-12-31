package com.example.messaging.core.pipeline.service;

import com.example.messaging.models.Message;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface PipelineManager {
    /**
     * Start the pipeline
     */
    void start();

    /**
     * Stop the pipeline
     */
    void stop();

    /**
     * Submit a message to the pipeline
     */
    CompletableFuture<Long> submitMessage(Message message);

    /**
     * Submit multiple messages to the pipeline
     */
    CompletableFuture<List<Long>> submitBatch(List<Message> messages);

    /**
     * Get current pipeline status
     */
    PipelineStatus getStatus();

    /**
     * Check if pipeline can accept more messages
     */
    boolean canAccept();

    enum PipelineStatus {
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        ERROR
    }
}

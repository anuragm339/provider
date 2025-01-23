package com.example.messaging.core.pipeline.service;

import com.example.messaging.models.Message;
import com.example.messaging.core.pipeline.impl.MessageDispatchOrchestrator;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface PipelineManager {
    /**
     * Submit a single message for processing
     * @param message Message to be processed
     * @return CompletableFuture with the message offset
     */
    CompletableFuture<Long> submitMessage(Message message);

    /**
     * Submit a batch of messages for processing
     * @param messages List of messages to be processed
     * @return CompletableFuture with list of message offsets
     */
    CompletableFuture<List<Long>> submitBatch(List<Message> messages);

    /**
     * Start the message dispatch orchestrator
     */
    void startDispatchOrchestrator();

    /**
     * Stop the message dispatch orchestrator
     */
    void stopDispatchOrchestrator();

    /**
     * Get current pipeline status
     * @return Current status of the pipeline
     */
    PipelineStatus getStatus();

    /**
     * Check if pipeline can accept more messages
     * @return true if pipeline can accept messages
     */
    boolean canAccept();

    /**
     * Possible pipeline statuses
     */
    enum PipelineStatus {
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        ERROR
    }

    void start();

    void stop();
}

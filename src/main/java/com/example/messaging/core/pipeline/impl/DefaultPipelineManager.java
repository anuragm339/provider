package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.PipelineManager;
import com.example.messaging.core.pipeline.service.MessageProcessor;
import com.example.messaging.core.pipeline.service.ProcessingResult;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.models.Message;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;

import jakarta.inject.Singleton;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Singleton
public class DefaultPipelineManager implements PipelineManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPipelineManager.class);

    private final MessageProcessor messageProcessor;
    private final MessageStore messageStore;
    private final MessageDispatchOrchestrator messageDispatchOrchestrator;
    private final DeadLetterQueueService deadLetterQueueService;

    // Track pipeline status
    private final AtomicReference<PipelineStatus> status =
            new AtomicReference<>(PipelineStatus.STOPPED);

    public DefaultPipelineManager(
            MessageProcessor messageProcessor,
            MessageStore messageStore,
            MessageDispatchOrchestrator messageDispatchOrchestrator,
            DeadLetterQueueService deadLetterQueueService
    ) {
        this.messageProcessor = messageProcessor;
        this.messageStore = messageStore;
        this.messageDispatchOrchestrator = messageDispatchOrchestrator;
        this.deadLetterQueueService = deadLetterQueueService;
    }

    @PostConstruct
    public void init() {
        startDispatchOrchestrator();
    }

    @Override
    public CompletableFuture<Long> submitMessage(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Check if pipeline can accept messages
                if (!canAccept()) {
                    throw new ProcessingException(
                            "Pipeline cannot accept messages",
                            ErrorCode.QUEUE_FULL.getCode(),
                            false
                    );
                }

                // Store the message
                messageStore.store(message).join();

                return message.getMsgOffset();

            } catch (Exception e) {
                // Handle processing errors
                logger.error("Message submission failed", e);

                // Move to Dead Letter Queue if processing fails
                deadLetterQueueService.enqueue(
                        message,
                        "Submission failed: " + e.getMessage()
                );

                throw new CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<List<Long>> submitBatch(List<Message> messages) {
        return CompletableFuture.supplyAsync(() ->
                messages.stream()
                        .map(this::submitMessage)
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
        );
    }

    @Override
    public void startDispatchOrchestrator() {
        if (status.compareAndSet(PipelineStatus.STOPPED, PipelineStatus.STARTING)) {
            try {
                messageDispatchOrchestrator.start();
                status.set(PipelineStatus.RUNNING);
                logger.info("Pipeline Dispatch Orchestrator started");
            } catch (Exception e) {
                status.set(PipelineStatus.ERROR);
                logger.error("Failed to start Dispatch Orchestrator", e);
            }
        }
    }

    @Override
    public void stopDispatchOrchestrator() {
        if (status.compareAndSet(PipelineStatus.RUNNING, PipelineStatus.STOPPING)) {
            try {
                messageDispatchOrchestrator.stop();
                status.set(PipelineStatus.STOPPED);
                logger.info("Pipeline Dispatch Orchestrator stopped");
            } catch (Exception e) {
                status.set(PipelineStatus.ERROR);
                logger.error("Failed to stop Dispatch Orchestrator", e);
            }
        }
    }

    @Override
    public PipelineStatus getStatus() {
        return status.get();
    }

    @Override
    public boolean canAccept() {
        return status.get() == PipelineStatus.RUNNING &&
                messageStore.canAccept();
    }

    @PreDestroy
    public void cleanup() {
        stopDispatchOrchestrator();
    }

    @Override
    public void start() {
        if (status.get() != PipelineStatus.STOPPED) {
            throw new IllegalStateException("Pipeline is not in STOPPED state");
        }
        status.set(PipelineStatus.RUNNING);
    }

    public void stop() {
        status.set(PipelineStatus.STOPPED);
        logger.debug("Pipeline stopped successfully");
    }
}

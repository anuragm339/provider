package com.example.messaging;

import com.example.messaging.core.pipeline.service.PipelineManager;
import com.example.messaging.transport.rsocket.server.MessageRSocketServer;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PipelineStartupListener {
    private static final Logger logger = LoggerFactory.getLogger(PipelineStartupListener.class);

    private final PipelineManager pipelineManager;
    private final MessageRSocketServer rSocketServer;

    public PipelineStartupListener(PipelineManager pipelineManager, MessageRSocketServer rSocketServer) {
        this.pipelineManager = pipelineManager;
        this.rSocketServer = rSocketServer;
    }

    @EventListener
    public void onStartup(StartupEvent event) {
        try {
            logger.debug("Starting message pipeline...");
            pipelineManager.start();

            logger.debug("Starting RSocket server...");
            rSocketServer.start().block();

            logger.debug("Message pipeline system initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize message pipeline system", e);
            throw new RuntimeException("Failed to initialize message pipeline system", e);
        }
    }
}

package com.example.messaging.transport.rsocket.server;

import com.example.messaging.core.pipeline.impl.MessageDispatchOrchestrator;
import com.example.messaging.transport.rsocket.consumer.ConsumerConnection;
import com.example.messaging.transport.rsocket.handler.ConsumerRequestHandler;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ConsumerRequestHandlerFactory {
    private final MessageCodec messageCodec;
    private final MessageDispatchOrchestrator messageDispatchOrchestrator;
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestHandlerFactory.class);

    @Inject
    public ConsumerRequestHandlerFactory(
            MessageCodec messageCodec,
            MessageDispatchOrchestrator messageDispatchOrchestrator) {
        this.messageCodec = messageCodec;
        this.messageDispatchOrchestrator = messageDispatchOrchestrator;
    }

    public ConsumerRequestHandler create(ConsumerConnection connection) {
        logger.info("Creating new ConsumerRequestHandler for consumer: {}", connection.getMetadata().getConsumerId());
        return new ConsumerRequestHandler(connection, messageCodec, messageDispatchOrchestrator);
    }
}

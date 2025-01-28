package com.example.messaging.transport.rsocket.server;

import com.example.messaging.core.pipeline.impl.MessageDispatchOrchestrator;
import com.example.messaging.exceptions.ConnectionException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.transport.rsocket.consumer.ConsumerConnection;
import com.example.messaging.transport.rsocket.consumer.ConsumerMetadata;
import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.handler.ConsumerRequestHandler;
import com.example.messaging.transport.rsocket.handler.ReplayRequestHandler;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import com.example.messaging.transport.rsocket.protocol.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

@Singleton
public class ConnectionAcceptor implements SocketAcceptor {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionAcceptor.class);

    private final ConsumerRegistry consumerRegistry;
    private final MessageCodec messageCodec;
    private final ConsumerRequestHandlerFactory handlerFactory;
    private final ObjectMapper objectMapper;
    private final MessageDispatchOrchestrator messageDispatchOrchestrator;

    @Inject
    public ConnectionAcceptor(
            ConsumerRegistry consumerRegistry,
            MessageCodec messageCodec,
            ConsumerRequestHandlerFactory handlerFactory,MessageDispatchOrchestrator messageDispatchOrchestrator) {
        this.consumerRegistry = consumerRegistry;
        this.messageCodec = messageCodec;
        this.handlerFactory = handlerFactory;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.messageDispatchOrchestrator=messageDispatchOrchestrator;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        return Mono.defer(() -> {
            try {
                ConsumerMetadata metadata = objectMapper.readValue(
                        setup.getMetadataUtf8(),
                        ConsumerMetadata.class
                );

                logger.info("Accepting new connection from consumer: {}",
                        metadata.getConsumerId());

                // Create consumer connection
                ConsumerConnection connection = new ConsumerConnection(
                        metadata,
                        sendingSocket,
                        messageCodec,messageDispatchOrchestrator
                );

                // Register consumer
                consumerRegistry.registerConsumer(connection);

                // Create handler using factory
                ConsumerRequestHandler handler = handlerFactory.create(connection);

                // Setup connection monitoring
                connection.onClose()
                        .doFinally(signalType -> {
                            logger.info("Connection closed for consumer: {}",
                                    metadata.getConsumerId());
                            consumerRegistry.unregisterConsumer(metadata.getConsumerId());
                        })
                        .subscribe();

                return Mono.just(handler);

            } catch (Exception e) {
                logger.error("Failed to establish connection", e);
                return Mono.error(new ConnectionException(
                        "Failed to establish connection",
                        ErrorCode.CONNECTION_FAILED.getCode(),
                        e
                ));
            }
        });
    }
}

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
import java.time.Duration;
import java.util.Map;

@Singleton
public class ConnectionAcceptor implements SocketAcceptor {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionAcceptor.class);
    private static final int PAYLOAD_SIZE_LIMIT = 1024 * 1024*10; // 1MB

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
                        // Check payload size
                        if (setup.data().readableBytes() > PAYLOAD_SIZE_LIMIT) {
                           return Mono.error(new ConnectionException("Payload size exceeds limit", ErrorCode.CONNECTION_FAILED.getCode()));
                        }

                        ConsumerMetadata metadata = objectMapper.readValue(
                                setup.getMetadataUtf8(),
                                ConsumerMetadata.class
                        );

                        // Check if consumer already exists
                        if (consumerRegistry.hasActiveConsumers(metadata.getConsumerId())) {
                            // Cleanup old connection first
                            consumerRegistry.unregisterConsumer(metadata.getConsumerId());
                        }

                        ConsumerConnection connection = new ConsumerConnection(
                                metadata,
                                sendingSocket,
                                messageCodec,
                                messageDispatchOrchestrator
                        );

                        // Set connection timeout
                        sendingSocket
                                .onClose()
                                .doFinally(__ -> {
                                    messageDispatchOrchestrator.cleanupConsumerResources(
                                            metadata.getGroupId(),
                                            metadata.getConsumerId()
                                    );
                                    consumerRegistry.unregisterConsumer(metadata.getConsumerId());
                                })
                                .subscribe();

                        consumerRegistry.registerConsumer(connection);

                        ConsumerRequestHandler consumerRequestHandler = handlerFactory.create(connection);
                        return  Mono.just((RSocket) consumerRequestHandler);

                    } catch (Exception e) {
                        return Mono.error(e);
                    }
                })
                .timeout(Duration.ofSeconds(3000))
                .doOnError(e -> logger.error("Connection setup failed", e));
    }
}

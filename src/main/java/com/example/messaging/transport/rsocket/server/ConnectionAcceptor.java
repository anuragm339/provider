package com.example.messaging.transport.rsocket.server;

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
    private final ReplayRequestHandler replayRequestHandler;

    @Inject
    public ConnectionAcceptor(
            ConsumerRegistry consumerRegistry,
            MessageCodec messageCodec,
            ReplayRequestHandler replayRequestHandler) {
        this.consumerRegistry = consumerRegistry;
        this.messageCodec = messageCodec;
        this.replayRequestHandler = replayRequestHandler;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        return Mono.defer(() -> {
            try {
                ConsumerMetadata metadata = new ObjectMapper().readValue(setup.getMetadataUtf8(), ConsumerMetadata.class);


                // Create consumer connection
                ConsumerConnection connection = new ConsumerConnection(metadata, sendingSocket, messageCodec);

                // Create handler
                ConsumerRequestHandler handler = new ConsumerRequestHandler(connection, messageCodec, replayRequestHandler);

                // Register consumer AFTER creating handler
                consumerRegistry.registerConsumer(connection);

                logger.info("Created RSocket handler for consumer: {}", metadata.getConsumerId());

                // This is important - return the handler
                return Mono.just(handler);
            } catch (Exception e) {
                logger.error("Failed to accept connection", e);
                return Mono.error(e);
            }
        });
    }

    private String getConsumerId(ConnectionSetupPayload setup) {
        String consumerId = setup.getMetadataUtf8();
        if (consumerId == null || consumerId.isEmpty()) {
            throw new ConnectionException(
                    "Consumer ID is required",
                    ErrorCode.CONNECTION_FAILED.getCode()
            );
        }
        return consumerId;
    }

    private String getGroupId(ConnectionSetupPayload setup) {
        String groupId = setup.getDataUtf8();
        if (groupId == null || groupId.isEmpty()) {
            groupId = "default"; // Fallback to default group
        }
        return groupId;
    }

    private boolean isValidProtocolVersion(ConnectionSetupPayload setup) {
        try {
            String version = setup.getMetadataUtf8();
            ObjectMapper objectMapper = new ObjectMapper();
            Map map = objectMapper.readValue(version, Map.class);
            return map!=null;
        } catch (Exception e) {
            System.out.println("Failed to validate protocol version: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

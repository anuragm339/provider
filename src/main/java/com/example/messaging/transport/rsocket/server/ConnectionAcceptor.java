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
import reactor.core.publisher.Mono;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

@Singleton
public class ConnectionAcceptor implements SocketAcceptor {

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
                String consumerMetaData = getConsumerId(setup);
                Map consumerConnection = new ObjectMapper().readValue(consumerMetaData, Map.class);

                // Validate protocol version
                if (!isValidProtocolVersion(setup)) {
                    return Mono.error(new ConnectionException(
                            "Unsupported protocol version",
                            ErrorCode.CONNECTION_FAILED.getCode()
                    ));
                }

                // Create consumer metadata and connection
                ConsumerMetadata metadata = new ConsumerMetadata(consumerMetaData, consumerConnection.get("groupId").toString());
                ConsumerConnection connection = new ConsumerConnection(metadata, sendingSocket, messageCodec);

                // Register the consumer
                consumerRegistry.registerConsumer(connection);

                // Create and return the request handler
                ConsumerRequestHandler handler = new ConsumerRequestHandler(
                        connection,
                        messageCodec,
                        replayRequestHandler
                );

                System.out.println("Consumer connected: " + consumerMetaData + " (group: " + consumerConnection + ")");
                return Mono.just(handler);

            } catch (Exception e) {
                System.out.println("Failed to establish connection: " + e.getMessage());
                e.printStackTrace();
                return Mono.error(new ConnectionException(
                        "Failed to establish connection",
                        ErrorCode.CONNECTION_FAILED.getCode(),
                        e
                ));
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

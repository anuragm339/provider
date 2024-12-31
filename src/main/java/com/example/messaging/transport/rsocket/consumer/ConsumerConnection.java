package com.example.messaging.transport.rsocket.consumer;

import com.example.messaging.exceptions.ConnectionException;
import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.models.Message;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class ConsumerConnection {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerConnection.class);

    private final ConsumerMetadata metadata;
    private final RSocket rSocket;
    private final MessageCodec messageCodec;
    private volatile boolean active;

    public ConsumerConnection(ConsumerMetadata metadata, RSocket rSocket, MessageCodec messageCodec) {
        this.metadata = metadata;
        this.rSocket = rSocket;
        this.messageCodec = messageCodec;
        this.active = true;

        // Setup connection cleanup
        setupConnectionCleanup();
    }

    public Mono<Void> sendMessage(TransportMessage message) {
        if (!active) {
            return Mono.error(new ConnectionException(
                    "Consumer connection is not active",
                    ErrorCode.CONSUMER_DISCONNECTED.getCode()
            ));
        }

        return Mono.defer(() -> {
                    try {
                        // Create a complete message structure
                        Map<String, Object> messageMap = new HashMap<>();
                        messageMap.put("messageId", message.getMessageId());
                        messageMap.put("timestamp", message.getTimestamp().toEpochMilli());
                        messageMap.put("message", new HashMap<String, Object>() {{
                            Message msg = message.getMessage();
                            put("offset", msg.getMsgOffset());
                            put("type", msg.getType());
                            put("data", Base64.getEncoder().encodeToString(msg.getData()));
                            put("createdUtc", msg.getCreatedUtc().toEpochMilli());
                        }});

                        // Serialize to JSON
                        String json = new ObjectMapper().writeValueAsString(messageMap);
                        logger.info("Sending message payload: ", json);

                        // Create payload
                        Payload payload = DefaultPayload.create(json);

                        // Send using requestResponse instead of fireAndForget to ensure delivery
                        return rSocket.requestResponse(payload)
                                .doOnSuccess(response -> {
                                    logger.info("Message sent successfully: " , message.getMessageId());
                                })
                                .then();
                    } catch (Exception e) {
                        return Mono.error(new ProcessingException(
                                "Failed to send message: " + e.getMessage(),
                                ErrorCode.PROCESSING_FAILED.getCode(),
                                true,
                                e
                        ));
                    }
                })
                .doOnError(error -> {
                    logger.info("Error sending message: " , error.getMessage());
                    handleSendError(error, message);
                })
                .retry(3) // Add retry logic
                .onErrorResume(error -> {
                    logger.info("Failed to send message after retries: " , error.getMessage());
                    return Mono.empty();
                });
    }

    private void handleSendError(Throwable error, TransportMessage message) {
        logger.error("Failed to send message to consumer: {} - Message: {}",
                metadata.getConsumerId(), message.getMessageId(), error);
    }

    private void setupConnectionCleanup() {
        rSocket.onClose()
                .doFinally(signalType -> {
                    active = false;
                    metadata.setState(ConsumerState.DISCONNECTED);
                    logger.info("Consumer connection closed: {}", metadata.getConsumerId());
                })
                .subscribe();
    }

    public void disconnect() {
        if (active) {
            active = false;
            metadata.setState(ConsumerState.DISCONNECTING);
            rSocket.dispose();
        }
    }

    public ConsumerMetadata getMetadata() {
        return metadata;
    }

    public boolean isActive() {
        return active;
    }
}

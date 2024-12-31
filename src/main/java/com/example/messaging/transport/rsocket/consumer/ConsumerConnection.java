package com.example.messaging.transport.rsocket.consumer;

import com.example.messaging.transport.rsocket.model.TransportMessage;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import io.rsocket.RSocket;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConnection {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerConnection.class);

    private final ConsumerMetadata metadata;
    private final RSocket rSocket;
    private final MessageCodec messageCodec;
    private final boolean active = true;

    public ConsumerConnection(ConsumerMetadata metadata, RSocket rSocket, MessageCodec messageCodec) {
        this.metadata = metadata;
        this.rSocket = rSocket;
        this.messageCodec = messageCodec;

        // Monitor connection
        rSocket.onClose()
                .doFinally(signalType -> {
                    logger.info("Consumer {} connection closed", metadata.getConsumerId());
                })
                .subscribe();
    }

    public Mono<Void> sendMessage(TransportMessage message) {
        if (!isActive()) {
            logger.warn("Attempt to send message to inactive consumer: {}", metadata.getConsumerId());
            return Mono.empty();
        }

        logger.info("Preparing to send message {} to consumer {}",
                message.getMessageId(), metadata.getConsumerId());

        return Mono.create(sink -> {
            try {
                logger.debug("Encoding message {} for consumer {}",
                        message.getMessageId(), metadata.getConsumerId());

                var encodedMessage = messageCodec.encodeMessage(message);

                logger.debug("Sending encoded message {} to consumer {}",
                        message.getMessageId(), metadata.getConsumerId());

                rSocket.requestResponse(io.rsocket.util.DefaultPayload.create(encodedMessage))
                        .doOnSuccess(__ -> {
                            logger.info("Successfully sent message {} to consumer {}",
                                    message.getMessageId(), metadata.getConsumerId());
                            sink.success();
                        })
                        .doOnError(error -> {
                            logger.error("Failed to send message {} to consumer {}: {}",
                                    message.getMessageId(), metadata.getConsumerId(), error.getMessage());
                            sink.error(error);
                        })
                        .subscribe();
            } catch (Exception e) {
                logger.error("Error preparing message {} for consumer {}",
                        message.getMessageId(), metadata.getConsumerId(), e);
                sink.error(e);
            }
        });
    }

    public ConsumerMetadata getMetadata() {
        return metadata;
    }

    public boolean isActive() {
        return active && !rSocket.isDisposed();
    }

    public void disconnect() {
        logger.info("Disconnecting consumer {}", metadata.getConsumerId());
        rSocket.dispose();
    }

    public Mono<Void> onClose() {
        return rSocket.onClose();
    }
}

package com.example.messaging.transport.rsocket.consumer;

import com.example.messaging.transport.rsocket.handler.ConsumerRequestHandler;
import com.example.messaging.transport.rsocket.handler.ReplayRequestHandler;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConnection {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerConnection.class);

    private final ConsumerMetadata metadata;
    private final RSocket rSocket;
    private final MessageCodec messageCodec;
    private final ConsumerRequestHandler requestHandler;

    public ConsumerConnection(ConsumerMetadata metadata, RSocket rSocket, MessageCodec messageCodec) {
        this.metadata = metadata;
        this.rSocket = rSocket;
        this.messageCodec = messageCodec;
        this.requestHandler = new ConsumerRequestHandler(this, messageCodec, null);

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

        logger.info("Preparing to send message {} to consumer {}", message.getMessageId(), metadata.getConsumerId());
        Payload messagePayload = DefaultPayload.create(messageCodec.encodeMessage(message));

        return rSocket.requestChannel(Flux.just(messagePayload))
                .doOnNext(response -> {
                    try {
                        String data = response.getDataUtf8();
                        logger.info("Received channel response: {}", data);
                        requestHandler.requestChannel(Flux.just(response)).subscribe();
                    } finally {
                        response.release();
                    }
                })
                .then(Mono.fromRunnable(() -> messagePayload.release()));
    }

    public ConsumerMetadata getMetadata() {
        return metadata;
    }

    public boolean isActive() {
        return !rSocket.isDisposed();
    }

    public void disconnect() {
        logger.info("Disconnecting consumer {}", metadata.getConsumerId());
        rSocket.dispose();
    }

    public Mono<Void> onClose() {
        return rSocket.onClose();
    }
}

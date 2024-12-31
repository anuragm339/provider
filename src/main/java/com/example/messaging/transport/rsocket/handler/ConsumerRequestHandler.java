package com.example.messaging.transport.rsocket.handler;

import com.example.messaging.transport.rsocket.consumer.ConsumerConnection;
import com.example.messaging.transport.rsocket.consumer.ConsumerState;
import com.example.messaging.transport.rsocket.model.ConsumerRequest;
import com.example.messaging.transport.rsocket.model.ReplayRequest;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import io.rsocket.RSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import jakarta.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerRequestHandler implements RSocket {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestHandler.class);

    private final ConsumerConnection connection;
    private final MessageCodec messageCodec;
    private final ReplayRequestHandler replayRequestHandler;

    public ConsumerRequestHandler(ConsumerConnection connection, MessageCodec messageCodec, ReplayRequestHandler replayRequestHandler) {
        this.connection = connection;
        this.messageCodec = messageCodec;
        this.replayRequestHandler = replayRequestHandler;
    }




    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux.defer(() -> {
            try {
                ConsumerRequest request = messageCodec.decodeRequest(payload);
                return handleStreamRequest(request);
            } finally {
                payload.release();
            }
        });
    }

    private Flux<Payload> handleStreamRequest(ConsumerRequest request) {
        switch (request.getType()) {
            case CONSUME:
                return handleConsumeRequest(request);
            case REPLAY:
                return handleReplayRequest((ReplayRequest) request);
            default:
                return Flux.error(new IllegalArgumentException("Unknown request type: " + request.getType()));
        }
    }

    private Flux<Payload> handleConsumeRequest(ConsumerRequest request) {
        connection.getMetadata().setState(ConsumerState.CONSUMING);
        logger.info("Consumer {} starting consumption", connection.getMetadata().getConsumerId());

        // Return a Flux that emits messages as they arrive
        return Flux.<Payload>create(sink -> {
            // Here you would set up message delivery to this consumer
            // This might involve registering a callback with the MessagePublisher
            // For now, just keep the stream open
            sink.onCancel(() -> {
                connection.getMetadata().setState(ConsumerState.CONNECTED);
                logger.info("Consumer {} stopped consumption", connection.getMetadata().getConsumerId());
            });
        });
    }

    private Flux<Payload> handleReplayRequest(ReplayRequest request) {
        connection.getMetadata().setState(ConsumerState.REPLAYING);
        return null;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.defer(() -> {
            try {
                ConsumerRequest request = messageCodec.decodeRequest(payload);
                return handleRequest(request);
            } finally {
                payload.release();
            }
        });
    }

    private Mono<Void> handleRequest(ConsumerRequest request) {
        switch (request.getType()) {
            case SUBSCRIBE:
                return handleSubscribe();
            case UNSUBSCRIBE:
                return handleUnsubscribe();
            case ACKNOWLEDGE:
                return handleAcknowledge(request);
            default:
                return Mono.error(new IllegalArgumentException("Unknown request type: " + request.getType()));
        }
    }

    private Mono<Void> handleSubscribe() {
        connection.getMetadata().setState(ConsumerState.ACTIVE);
        logger.info("Consumer subscribed: {}", connection.getMetadata().getConsumerId());
        return Mono.empty();
    }

    private Mono<Void> handleUnsubscribe() {
        connection.getMetadata().setState(ConsumerState.DISCONNECTING);
        logger.info("Consumer unsubscribed: {}", connection.getMetadata().getConsumerId());
        return Mono.empty();
    }

    private Mono<Void> handleAcknowledge(ConsumerRequest request) {
        logger.debug("Received acknowledgment from consumer: {}",
                connection.getMetadata().getConsumerId());
        return Mono.empty();
    }
}

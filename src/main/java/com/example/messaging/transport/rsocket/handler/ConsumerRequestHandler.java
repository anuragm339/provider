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
import org.reactivestreams.Publisher;
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
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(payloads)
                .doOnNext(payload -> {
                    try {
                        String data = payload.getDataUtf8();
                        logger.debug("Received ack in provider: {}", data);
                        // Process acknowledgment
                    } finally {
                        payload.release();
                    }
                });
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.defer(() -> {
            try {
                String data = payload.getDataUtf8();
                logger.debug("Received fire-and-forget request: {}", data);
                ConsumerRequest request = messageCodec.decodeRequest(payload);
                return handleRequest(request);
            } finally {
                payload.release();
            }
        });
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux.defer(() -> {
            try {
                String data = payload.getDataUtf8();
                logger.debug("Received stream request: {}", data);
                ConsumerRequest request = messageCodec.decodeRequest(payload);
                return handleStreamRequest(request);
            } finally {
                payload.release();
            }
        });
    }

    private Mono<Void> handleRequest(ConsumerRequest request) {
        logger.debug("Handling request: {}", request.getType());
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

    private Flux<Payload> handleStreamRequest(ConsumerRequest request) {
        logger.debug("Handling stream request: {}", request.getType());
        switch (request.getType()) {
            case CONSUME:
                return handleConsumeRequest(request);
            case REPLAY:
                return handleReplayRequest((ReplayRequest) request);
            default:
                return Flux.error(new IllegalArgumentException("Unknown request type: " + request.getType()));
        }
    }

    private Mono<Void> handleSubscribe() {
        connection.getMetadata().setState(ConsumerState.ACTIVE);
        logger.debug("Consumer subscribed: {}", connection.getMetadata().getConsumerId());
        return Mono.empty();
    }

    private Mono<Void> handleUnsubscribe() {
        connection.getMetadata().setState(ConsumerState.DISCONNECTING);
        logger.debug("Consumer unsubscribed: {}", connection.getMetadata().getConsumerId());
        return Mono.empty();
    }

    private Mono<Void> handleAcknowledge(ConsumerRequest request) {
        logger.debug("Received acknowledgment from consumer: {}", connection.getMetadata().getConsumerId());
        return Mono.empty();
    }

    private Flux<Payload> handleConsumeRequest(ConsumerRequest request) {
        connection.getMetadata().setState(ConsumerState.CONSUMING);
        logger.debug("Starting consumption for consumer: {}", connection.getMetadata().getConsumerId());
        return Flux.empty(); // Keep stream open for messages
    }

    private Flux<Payload> handleReplayRequest(ReplayRequest request) {
        connection.getMetadata().setState(ConsumerState.REPLAYING);
        return Flux.empty(); // Implement replay logic
    }
}

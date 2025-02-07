package com.example.messaging.transport.rsocket.handler;

import com.example.messaging.core.pipeline.impl.MessageDispatchOrchestrator;
import com.example.messaging.core.pipeline.service.MessageProcessor;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.transport.rsocket.consumer.ConsumerConnection;
import com.example.messaging.transport.rsocket.consumer.ConsumerState;
import com.example.messaging.transport.rsocket.model.ConsumerRequest;
import com.example.messaging.transport.rsocket.model.ReplayRequest;
import com.example.messaging.transport.rsocket.protocol.MessageCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.rsocket.RSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class ConsumerRequestHandler implements RSocket {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRequestHandler.class);

    private final ConsumerConnection connection;
    private final MessageCodec messageCodec;
    private final MessageDispatchOrchestrator messageDispatchOrchestrator;

    public ConsumerRequestHandler(ConsumerConnection connection, MessageCodec messageCodec, MessageDispatchOrchestrator messageDispatchOrchestrator) {
        this.connection = connection;
        this.messageCodec = messageCodec;
        this.messageDispatchOrchestrator = messageDispatchOrchestrator;
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(payloads)
                .doOnNext(payload -> {
                    try {
                        String data = payload.getDataUtf8();
                        JsonNode root;
                        String type;
                        try {
                            root = new ObjectMapper().readTree(data);
                            type = root.get("type").asText();
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                        if ("BATCH_ACK".equals(type)) {
                            String batchId = root.get("batchId").asText();

                            JsonNode offsetsNode = root.get("messageOffsets");
                            List<Long> ackOffsetList=new ArrayList<>();
                            if (offsetsNode.isArray()) {
                                offsetsNode.forEach(offset -> ackOffsetList.add(offset.asLong()));
                                String consumerId = extractConsumerId(batchId);
                                messageDispatchOrchestrator.handleBatchAcknowledgment(
                                        batchId,
                                        ackOffsetList,
                                        consumerId
                                );
                            }
                            logger.info("Received ack in provider: {}", batchId);
                        }
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
    private String extractConsumerId(String input){
        Pattern pattern = Pattern.compile("^(type-\\d+)");
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}

package com.example.messaging.transport.rsocket.handler;

import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.transport.rsocket.consumer.ConsumerConnection;
import com.example.messaging.transport.rsocket.model.ReplayRequest;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@Singleton
public class ReplayRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(ReplayRequestHandler.class);

    private final MessageStore messageStore;
    private static final int DEFAULT_BATCH_SIZE = 100;

    @Inject
    public ReplayRequestHandler(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public Flux<Void> handleReplayRequest(ReplayRequest request, ConsumerConnection connection) {
        logger.info("Processing replay request for consumer: {} from offset {} to {}",
                connection.getMetadata().getConsumerId(),
                request.getFromOffset(),
                request.getToOffset());

        int batchSize = request.getBatchSize() > 0 ? request.getBatchSize() : DEFAULT_BATCH_SIZE;

        return Flux.range(0, calculateBatches(request, batchSize))
                .flatMap(batchIndex -> processBatch(request, batchIndex, batchSize, connection))
                .doOnComplete(() ->
                        logger.info("Replay completed for consumer: {}",
                                connection.getMetadata().getConsumerId()))
                .doOnError(error ->
                        logger.error("Error during replay for consumer: {}",
                                connection.getMetadata().getConsumerId(), error));
    }

    private int calculateBatches(ReplayRequest request, int batchSize) {
        long messageCount = request.getToOffset() - request.getFromOffset() + 1;
        return (int) Math.ceil((double) messageCount / batchSize);
    }

    private Mono<Void> processBatch(ReplayRequest request, int batchIndex, int batchSize,
                                    ConsumerConnection connection) {
        long startOffset = request.getFromOffset() + (batchIndex * batchSize);
        long endOffset = Math.min(startOffset + batchSize - 1, request.getToOffset());

        return Flux.range((int)startOffset, (int)(endOffset - startOffset + 1))
                .flatMap(offset -> Mono.fromFuture(messageStore.getMessage(offset))
                        .flatMap(messageOpt -> {
                            if (messageOpt.isPresent()) {
                                TransportMessage transportMessage =
                                        new TransportMessage(messageOpt.get());
                                return connection.sendMessage(transportMessage);
                            }
                            return Mono.empty();
                        }))
                .then()
                .doOnSuccess(v ->
                        logger.debug("Processed batch {}: {} to {} for consumer: {}",
                                batchIndex, startOffset, endOffset,
                                connection.getMetadata().getConsumerId()));
    }
}

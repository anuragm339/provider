package com.example.messaging.transport.rsocket.handler;

import com.example.messaging.models.BatchMessage;
import com.example.messaging.models.Message;
import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

@Singleton
public class MessagePublisher {
    private static final Logger logger = LoggerFactory.getLogger(MessagePublisher.class);

    private final ConsumerRegistry consumerRegistry;
    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(10);

    @Inject
    public MessagePublisher(ConsumerRegistry consumerRegistry) {
        this.consumerRegistry = consumerRegistry;
    }

    public Mono<Void> publishBatchMessage(BatchMessage batchMessage, String groupId) {
        logger.debug("Publishing message {} to group {} {}", batchMessage.getBatchId(), batchMessage.getType(),groupId);

        try {
            TransportMessage transportMessage = new TransportMessage(batchMessage);
            return consumerRegistry.broadcastToGroup(groupId, transportMessage)
                    .timeout(PUBLISH_TIMEOUT)
                    .doOnSubscribe(__ ->
                            logger.debug("Starting broadcast of message {} to group {}",
                                    batchMessage.getBatchId(), groupId))
                    .doOnSuccess(__ ->
                            logger.debug("Successfully broadcast message {} to group {}",
                                    batchMessage.getBatchId(), groupId))
                    .doOnError(error ->
                            logger.error("Failed to broadcast message {} to group {}: {}",
                                    batchMessage.getBatchId(), groupId, error.getMessage()))
                    .onErrorResume(error -> {
                        logger.error("Error during broadcast, ensuring cleanup", error);
                        return Mono.empty();
                    });
        } catch (Exception e) {
            logger.error("Error creating transport message for {}", batchMessage.getBatchId(), e);
            return Mono.error(e);
        }
    }
    public Mono<Void> publishMessage(Message message, String groupId) {
        logger.debug("Publishing message {} to group {}", message.getMsgOffset(), groupId);

        try {
            TransportMessage transportMessage = new TransportMessage(message);
            return consumerRegistry.broadcastToGroup(groupId, transportMessage)
                    .timeout(PUBLISH_TIMEOUT)
                    .doOnSubscribe(__ ->
                            logger.debug("Starting broadcast of message {} to group {}",
                                    message.getMsgOffset(), groupId))
                    .doOnSuccess(__ ->
                            logger.debug("Successfully broadcast message {} to group {}",
                                    message.getMsgOffset(), groupId))
                    .doOnError(error ->
                            logger.error("Failed to broadcast message {} to group {}: {}",
                                    message.getMsgOffset(), groupId, error.getMessage()))
                    .onErrorResume(error -> {
                        logger.error("Error during broadcast, ensuring cleanup", error);
                        return Mono.empty();
                    });
        } catch (Exception e) {
            logger.error("Error creating transport message for {}", message.getMsgOffset(), e);
            return Mono.error(e);
        }
    }

    public Mono<Void> publishToConsumer(Message message, String consumerId) {
        logger.debug("Publishing message {} to consumer {}", message.getMsgOffset(), consumerId);

        try {
            TransportMessage transportMessage = new TransportMessage(message);
            return consumerRegistry.sendToConsumer(consumerId, transportMessage)
                    .timeout(PUBLISH_TIMEOUT)
                    .doOnSubscribe(__ ->
                            logger.debug("Starting send of message {} to consumer {}",
                                    message.getMsgOffset(), consumerId))
                    .doOnSuccess(__ ->
                            logger.debug("Successfully sent message {} to consumer {}",
                                    message.getMsgOffset(), consumerId))
                    .doOnError(error ->
                            logger.error("Failed to send message {} to consumer {}: {}",
                                    message.getMsgOffset(), consumerId, error.getMessage()))
                    .onErrorResume(error -> {
                        logger.error("Error during send, ensuring cleanup", error);
                        return Mono.empty();
                    });
        } catch (Exception e) {
            logger.error("Error creating transport message for {}", message.getMsgOffset(), e);
            return Mono.error(e);
        }
    }
}

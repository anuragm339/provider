package com.example.messaging.transport.rsocket.handler;

import com.example.messaging.models.Message;
import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class MessagePublisher {
    private static final Logger logger = LoggerFactory.getLogger(MessagePublisher.class);

    private final ConsumerRegistry consumerRegistry;

    @Inject
    public MessagePublisher(ConsumerRegistry consumerRegistry) {
        this.consumerRegistry = consumerRegistry;
    }

    public Mono<Void> publishMessage(Message message, String groupId) {
        TransportMessage transportMessage = new TransportMessage(message);

        return consumerRegistry.broadcastToGroup(groupId, transportMessage)
                .doOnSuccess(v ->
                        logger.debug("Message {} published to group {}",
                                message.getMsgOffset(), groupId))
                .doOnError(error ->
                        logger.error("Failed to publish message {} to group {}",
                                message.getMsgOffset(), groupId, error));
    }

    public Mono<Void> publishToConsumer(Message message, String consumerId) {
        TransportMessage transportMessage = new TransportMessage(message);

        return consumerRegistry.sendToConsumer(consumerId, transportMessage)
                .doOnSuccess(v ->
                        logger.debug("Message {} published to consumer {}",
                                message.getMsgOffset(), consumerId))
                .doOnError(error ->
                        logger.error("Failed to publish message {} to consumer {}",
                                message.getMsgOffset(), consumerId, error));
    }
}

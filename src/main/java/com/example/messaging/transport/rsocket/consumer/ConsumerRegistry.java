package com.example.messaging.transport.rsocket.consumer;

import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import jakarta.inject.Singleton;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

@Singleton
public class ConsumerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRegistry.class);

    private final Map<String, ConsumerConnection> consumerConnections;
    private final Map<String, Set<String>> consumerGroups;
    private final Queue<PendingMessage> pendingMessages = new ConcurrentLinkedQueue<>();


    public ConsumerRegistry() {
        this.consumerConnections = new ConcurrentHashMap<>();
        this.consumerGroups = new ConcurrentHashMap<>();
    }

    public void registerConsumer(ConsumerConnection connection) {
        ConsumerMetadata metadata = connection.getMetadata();
        String consumerId = metadata.getConsumerId();
        String groupId = metadata.getGroupId();

        consumerConnections.put(consumerId, connection);
        consumerGroups.computeIfAbsent(groupId, k -> ConcurrentHashMap.newKeySet())
                .add(consumerId);

        logger.debug("Consumer registered - ID: {}, Group: {}", consumerId, groupId);
        // Process any pending messages for this consumer's group
        deliverPendingMessages(groupId);
    }

    public void unregisterConsumer(String consumerId) {
        ConsumerConnection connection = consumerConnections.remove(consumerId);
        if (connection != null) {
            String groupId = connection.getMetadata().getGroupId();
            consumerGroups.computeIfPresent(groupId, (k, members) -> {
                members.remove(consumerId);
                return members.isEmpty() ? null : members;
            });

            connection.disconnect();
            logger.debug("Consumer unregistered - ID: {}, Group: {}", consumerId, groupId);
        }
    }

    public Mono<Void> broadcastToGroup(String groupId, TransportMessage message) {
        Set<String> consumers = getGroupMembers(groupId);
        if (consumers.isEmpty()) {
            logger.debug("No active consumers found for group: {}. Message will be queued.", groupId);
            pendingMessages.offer(new PendingMessage(message, groupId));
            return Mono.empty();
        }

        logger.debug("Starting broadcast to group: {} for message: {}, Active consumers: {}",
                groupId, message.getMessageId(), consumers.size());

        return Flux.fromIterable(consumers)
                .flatMap(consumerId -> {
                    ConsumerConnection connection = consumerConnections.get(consumerId);
                    if (connection != null && connection.isActive()) {
                        return connection.sendMessage(message)
                                .doOnSuccess(__ -> logger.debug("Successfully sent message to consumer {}", consumerId))
                                .doOnError(error -> logger.error("Failed to send message to consumer {}: {}",
                                        consumerId, error.getMessage()));
                    }
                    return Mono.empty();
                })
                .then();
    }

    public Mono<Void> sendToConsumer(String consumerId, TransportMessage message) {
        ConsumerConnection connection = consumerConnections.get(consumerId);
        if (connection != null && connection.isActive()) {
            logger.debug("Found active connection for consumer {}", consumerId);
            return connection.sendMessage(message)
                    .doOnError(error -> {
                        logger.error("Error sending message to consumer {}: {}",
                                consumerId, error.getMessage());
                        if (!connection.isActive()) {
                            logger.warn("Connection no longer active for consumer {}, unregistering",
                                    consumerId);
                            unregisterConsumer(consumerId);
                        }
                    });
        } else {
            logger.warn("No active connection found for consumer: {}", consumerId);
            if (connection != null && !connection.isActive()) {
                unregisterConsumer(consumerId);
            }
            return Mono.empty();
        }
    }

    private Set<String> getGroupMembers(String groupId) {
        return consumerGroups.getOrDefault(groupId, Collections.emptySet());
    }

    public Stream<ConsumerConnection> getActiveConnections() {
        return consumerConnections.values().stream()
                .filter(ConsumerConnection::isActive);
    }

    private static class PendingMessage {
        final TransportMessage message;
        final String groupId;

        PendingMessage(TransportMessage message, String groupId) {
            this.message = message;
            this.groupId = groupId;
        }
    }

    private void deliverPendingMessages(String groupId) {
        logger.debug("Checking pending messages for group: {}", groupId);

        pendingMessages.removeIf(pending -> {
            if (pending.groupId.equals(groupId)) {
                Set<String> consumers = getGroupMembers(groupId);
                if (!consumers.isEmpty()) {
                    logger.debug("Delivering pending message to group: {}", groupId);
                    broadcastToGroup(groupId, pending.message).subscribe();
                    return true; // Remove this message from pending queue
                }
            }
            return false;
        });
    }
}

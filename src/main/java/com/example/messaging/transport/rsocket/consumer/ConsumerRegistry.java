package com.example.messaging.transport.rsocket.consumer;

import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import jakarta.inject.Singleton;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.stream.Stream;

@Singleton
public class ConsumerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRegistry.class);

    private final Map<String, ConsumerConnection> consumerConnections;
    private final Map<String, Set<String>> consumerGroups;

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

        logger.info("Consumer registered - ID: {}, Group: {}", consumerId, groupId);
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
            logger.info("Consumer unregistered - ID: {}, Group: {}", consumerId, groupId);
        }
    }

    public Mono<Void> broadcastToGroup(String groupId, TransportMessage message) {
        Set<String> groupMembers = getGroupMembers(groupId);

        if (groupMembers.isEmpty()) {
            return Mono.error(new ProcessingException(
                    "No active consumers in group: " + groupId,
                    ErrorCode.NO_ACTIVE_CONSUMERS.getCode(),
                    true
            ));
        }
        return Flux.fromIterable(getGroupMembers(groupId))
                .flatMap(consumerId -> sendToConsumer(consumerId, message))
                .then();
    }

    public Mono<Void> sendToConsumer(String consumerId, TransportMessage message) {
        ConsumerConnection connection = consumerConnections.get(consumerId);
        if (connection != null && connection.isActive()) {
            logger.info("ConsumerRegistry.sendToConsumer {} "+message.getMessage());
            return connection.sendMessage(message);
        }
        return Mono.empty();
    }

    private Set<String> getGroupMembers(String groupId) {
        return consumerGroups.getOrDefault(groupId, Collections.emptySet());
    }

    public Stream<ConsumerConnection> getActiveConnections() {
        return consumerConnections.values().stream()
                .filter(ConsumerConnection::isActive);
    }
}

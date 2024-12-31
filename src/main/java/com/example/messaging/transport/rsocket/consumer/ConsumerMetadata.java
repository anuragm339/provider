package com.example.messaging.transport.rsocket.consumer;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerMetadata {
    private final String consumerId;
    private final String groupId;
    private final Map<String, String> properties;
    private final Instant connectedAt;
    private ConsumerState state;

    public ConsumerMetadata(String consumerId, String groupId) {
        this.consumerId = consumerId;
        this.groupId = groupId;
        this.properties = new ConcurrentHashMap<>();
        this.connectedAt = Instant.now();
        this.state = ConsumerState.CONNECTED;
    }



    // Getters and state management methods
    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }
    public Instant getConnectedAt() { return connectedAt; }
    public ConsumerState getState() { return state; }

    public void setState(ConsumerState state) {
        this.state = state;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public String getProperty(String key) {
        return properties.get(key);
    }
}

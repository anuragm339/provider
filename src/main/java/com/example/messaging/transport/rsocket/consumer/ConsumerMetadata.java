package com.example.messaging.transport.rsocket.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerMetadata {
    private  String consumerId;
    private  String groupId;
    private  Map<String, String> properties;
    private  Instant connectedAt;
    private ConsumerState state;

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setConnectedAt(Instant connectedAt) {
        this.connectedAt = connectedAt;
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

package com.example.messaging.transport.rsocket.model;


import com.example.messaging.transport.rsocket.protocol.RequestType;

import java.time.Instant;

public class ConsumerRequest {
    private final String consumerId;
    private final RequestType type;
    private final Instant timestamp;
    private final String metadata;

    public ConsumerRequest(String consumerId, RequestType type, String metadata) {
        this.consumerId = consumerId;
        this.type = type;
        this.timestamp = Instant.now();
        this.metadata = metadata;
    }

    // Getters
    public String getConsumerId() { return consumerId; }
    public RequestType getType() { return type; }
    public Instant getTimestamp() { return timestamp; }
    public String getMetadata() { return metadata; }
}

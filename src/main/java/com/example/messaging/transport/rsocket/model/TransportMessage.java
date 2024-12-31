package com.example.messaging.transport.rsocket.model;

import com.example.messaging.models.Message;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TransportMessage {
    private final Message message;
    private final String messageId;
    private final Instant timestamp;
    private final Map<String, String> attributes;

    public TransportMessage(Message message) {
        this.message = message;
        this.messageId = generateMessageId();
        this.timestamp = Instant.now();
        this.attributes = new HashMap<>();
    }

    private String generateMessageId() {
        return UUID.randomUUID().toString();
    }

    public Message getMessage() {
        return message;
    }

    public String getMessageId() {
        return messageId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
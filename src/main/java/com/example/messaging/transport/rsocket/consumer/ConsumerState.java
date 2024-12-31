package com.example.messaging.transport.rsocket.consumer;

public enum ConsumerState {
    CONNECTED,
    SUBSCRIBING,
    ACTIVE,
    REPLAYING,
    DISCONNECTING,
    DISCONNECTED,
    CONSUMING
}

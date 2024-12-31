package com.example.messaging.transport.rsocket.protocol;

public enum RequestType {
    SUBSCRIBE,
    UNSUBSCRIBE,
    REPLAY,
    ACKNOWLEDGE,
    HEALTH_CHECK,
    CONSUME
}

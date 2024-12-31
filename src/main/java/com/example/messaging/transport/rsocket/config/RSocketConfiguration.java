package com.example.messaging.transport.rsocket.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties("rsocket")
public interface RSocketConfiguration {

    @Bindable(defaultValue = "7000")
    int getPort();

    @Bindable(defaultValue = "TCP")
    String getTransport();

    @Bindable(defaultValue = "1000")
    int getMaxInboundPayloadSize();
}

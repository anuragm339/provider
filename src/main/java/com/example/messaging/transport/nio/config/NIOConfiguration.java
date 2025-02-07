package com.example.messaging.transport.nio.config;


import com.example.messaging.transport.nio.buffer.BufferPool;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;

@Factory
public class NIOConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(NIOConfiguration.class);

    @Singleton
    public Selector selector() throws IOException {
        logger.info("Creating NIO Selector");
        return Selector.open();
    }
}

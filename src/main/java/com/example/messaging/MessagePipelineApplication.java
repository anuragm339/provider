package com.example.messaging;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;

public class MessagePipelineApplication {
    public static void main(String[] args) {
        ApplicationContext applicationContext = Micronaut.run(MessagePipelineApplication.class, args);
    }
}

package com.example.messaging.config;

import com.example.messaging.core.pipeline.config.QueueConfiguration;
import com.example.messaging.core.pipeline.impl.DeadLetterQueueService;
import com.example.messaging.core.pipeline.impl.DefaultPipelineManager;
import com.example.messaging.core.pipeline.impl.MessageDispatchOrchestrator;
import com.example.messaging.core.pipeline.service.MessageProcessor;
import com.example.messaging.core.pipeline.service.BatchProcessor;
import com.example.messaging.core.pipeline.service.PipelineManager;
import com.example.messaging.storage.service.MessageStore;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Primary;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Factory
public class PipelineManagerFactory {

    @ConfigurationProperties("pipeline.manager")
    public static class PipelineManagerConfig {
        private int queueCapacity = 10000;
        private int threadPoolSize = Runtime.getRuntime().availableProcessors()*4;

        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
        public int getThreadPoolSize() { return threadPoolSize; }
        public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }
    }

    @Singleton
    @Named("pipelineWorker")
    public ExecutorService customExecutor() {
        return Executors.newFixedThreadPool(10); // Example configuration
    }

    @Singleton
    @Primary
    public ExecutorService primaryExecutor() {
        return Executors.newFixedThreadPool(10);
    }
    @Singleton
    public PipelineManager pipelineManager(
            MessageProcessor messageProcessor,
            MessageDispatchOrchestrator MessageDispatchOrchestrator,
            DeadLetterQueueService deadLetterQueueService,
            MessageStore messageStore,
            PipelineManagerConfig config, QueueConfiguration queueConfiguration) {

        return new DefaultPipelineManager(
                messageProcessor,
                messageStore,
                MessageDispatchOrchestrator,
                deadLetterQueueService
        );
    }
}

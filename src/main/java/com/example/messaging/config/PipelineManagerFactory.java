package com.example.messaging.config;

import com.example.messaging.core.pipeline.config.QueueConfiguration;
import com.example.messaging.core.pipeline.impl.DeadLetterQueueService;
import com.example.messaging.core.pipeline.impl.DefaultPipelineManager;
import com.example.messaging.core.pipeline.impl.MessageDispatchOrchestrator;
import com.example.messaging.core.pipeline.service.MessageProcessor;
import com.example.messaging.core.pipeline.service.BatchProcessor;
import com.example.messaging.core.pipeline.service.PipelineManager;
import com.example.messaging.storage.service.MessageStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micronaut.context.annotation.*;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

@Factory
public class PipelineManagerFactory {

    private static final String THREAD_NAME_PREFIX = "message-pipeline";

    private static final Logger logger = LoggerFactory.getLogger(PipelineManagerFactory.class);


    public static class PipelineManagerConfig {
        @Value("${pipeline.manager.queueCapacity:1000}")
        private int queueCapacity;
        @Value("${pipeline.manager.threadPoolSize:4}")
        private int threadPoolSize;
        @Value("${pipeline.manager.corePoolSize:4}")
        private int corePoolSize;
        @Value("${pipeline.manager.maxpollSize:10}")
        private int maxPoolSize;
        @Value("${pipeline.manager.keepAliveTime:1000}")
        private int keepAliveTime;
        public PipelineManagerConfig() {}
        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
        public int getThreadPoolSize() { return threadPoolSize; }
        public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }

        public int getCorePoolSize() {
            return this.corePoolSize;
        }

        public int getMaxPoolSize() {
            return this.maxPoolSize;
        }

        public long getKeepAliveTime() {
            return this.keepAliveTime;
        }
    }

    @Singleton
    @Named("pipelineWorker")
    public ExecutorService pipeLineExecuter(PipelineManagerConfig pipelineManagerConfig) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("pipeline-worker-%d")
                .setUncaughtExceptionHandler((thread, throwable) ->
                        logger.error("Uncaught error in thread {}: {}", thread.getName(), throwable.getMessage(), throwable))
                .build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                pipelineManagerConfig.getCorePoolSize(),
                pipelineManagerConfig.getMaxPoolSize(),
                pipelineManagerConfig.getKeepAliveTime(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(pipelineManagerConfig.getQueueCapacity()),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        logger.info("Created pipeline worker executor - core: {}, max: {}, queue: {}", pipelineManagerConfig.getCorePoolSize(), pipelineManagerConfig.getMaxPoolSize(), pipelineManagerConfig.getQueueCapacity());

        return executor;
    }

    @Singleton
    @Named("messageProcessorWorker")
    public ExecutorService messageProcessor(PipelineManagerConfig pipelineManagerConfig) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("pipeline-worker-%d")
                .setUncaughtExceptionHandler((thread, throwable) ->
                        logger.error("Uncaught error in thread {}: {}", thread.getName(), throwable.getMessage(), throwable))
                .build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                pipelineManagerConfig.getCorePoolSize(),
                pipelineManagerConfig.getMaxPoolSize(),
                pipelineManagerConfig.getKeepAliveTime(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(pipelineManagerConfig.getQueueCapacity()),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        logger.info("Created message proccessor worker executor - core: {}, max: {}, queue: {}", pipelineManagerConfig.getCorePoolSize(), pipelineManagerConfig.getMaxPoolSize(), pipelineManagerConfig.getQueueCapacity());

        return executor;
    }

    @Singleton
    @Named("messageStoreWorker")
    public ExecutorService messageStoreWorker(PipelineManagerConfig pipelineManagerConfig) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("message-store-worker-%d")
                .setUncaughtExceptionHandler((thread, throwable) ->
                        logger.error("Uncaught error in thread {}: {}", thread.getName(), throwable.getMessage(), throwable))
                .build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                pipelineManagerConfig.getCorePoolSize(),
                pipelineManagerConfig.getMaxPoolSize(),
                pipelineManagerConfig.getKeepAliveTime(), TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(pipelineManagerConfig.getQueueCapacity()),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        logger.info("Created message storage worker executor - core: {}, max: {}, queue: {}", pipelineManagerConfig.getCorePoolSize(), pipelineManagerConfig.getMaxPoolSize(), pipelineManagerConfig.getQueueCapacity());

        return executor;
    }

    @Singleton
    public PipelineManager pipelineManager(
            MessageProcessor messageProcessor,
            MessageDispatchOrchestrator MessageDispatchOrchestrator,
            DeadLetterQueueService deadLetterQueueService,
            MessageStore messageStore,
           @Named("pipelineWorker") ExecutorService pipelineWorker) {
        return new DefaultPipelineManager(
                messageProcessor,
                messageStore,
                MessageDispatchOrchestrator,
                deadLetterQueueService,
                pipelineWorker

        );
    }
}

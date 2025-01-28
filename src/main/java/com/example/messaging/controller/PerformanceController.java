package com.example.messaging.controller;

import com.example.messaging.core.pipeline.service.PipelineManager;
import com.example.messaging.models.Message;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Controller("/api/performance")
public class PerformanceController {
    private static final Logger logger = LoggerFactory.getLogger(PerformanceController.class);
    private final PipelineManager pipelineManager;
    private final ExecutorService executorService;
    private final AtomicLong messageCounter;
    private final Map<String, AtomicLong> typeCounters;

    @Inject
    public PerformanceController(PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
        this.executorService = Executors.newCachedThreadPool();
        this.messageCounter = new AtomicLong(0);
        this.typeCounters = new ConcurrentHashMap<>();
    }

    @Post("/parallel-test")
    public HttpResponse<String> runParallelTest(
            @QueryValue(defaultValue = "14") int numberOfConsumers,
            @QueryValue(defaultValue = "1000") int messagesPerConsumer,
            @QueryValue(defaultValue = "1024") int messageSize) {

        long startTime = System.currentTimeMillis();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Create test data
        byte[] testData = new byte[messageSize];
        Arrays.fill(testData, (byte) 'X');

        // Start consumers in parallel
        for (int i = 0; i < numberOfConsumers; i++) {
            final String consumerType = "type-" + i;  // Each consumer gets its own type
            typeCounters.putIfAbsent(consumerType, new AtomicLong(0));

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    logger.info("Starting consumer for type: {}", consumerType);

                    for (int j = 0; j < messagesPerConsumer; j++) {
                        Message message = Message.builder()
                                .msgKey(UUID.randomUUID().toString())
                                .msgOffset(messageCounter.incrementAndGet())
                                .type(consumerType)
                                .createdUtc(Instant.now())
                                .data(testData)
                                .build();

                        typeCounters.get(consumerType).incrementAndGet();

                        pipelineManager.submitMessage(message)
                                .whenComplete((result, error) -> {
                                    if (error != null) {
                                        logger.error("Error processing message for type {}: {}",
                                                consumerType, error.getMessage());
                                    }
                                })
                                .join();

                        // Add small delay between messages
                        if (j % 100 == 0) {
                            Thread.sleep(10);
                        }
                    }
                    logger.info("Completed processing all messages for type {}", consumerType);
                } catch (Exception e) {
                    logger.error("Error processing type {}: {}", consumerType, e.getMessage());
                    throw new RuntimeException(e);
                }
            }, executorService);

            futures.add(future);
        }

        // Wait for all consumers to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        long totalMessages = (long) numberOfConsumers * messagesPerConsumer;
        double messagesPerSecond = totalMessages / (totalTime / 1000.0);

        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append(String.format("""
            Performance Test Results:
            Consumers: %d
            Messages per consumer: %d
            Message size: %d bytes
            Total messages: %d
            Total time: %dms
            Throughput: %.2f messages/second
            
            Messages by Type:
            """,
                numberOfConsumers, messagesPerConsumer, messageSize,
                totalMessages, totalTime, messagesPerSecond));

        typeCounters.forEach((type, counter) -> {
            resultBuilder.append(String.format("%s: %d messages%n", type, counter.get()));
        });

        String result = resultBuilder.toString();
        logger.info(result);
        return HttpResponse.ok(result);
    }

    @Get("/status")
    public HttpResponse<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("totalMessages", messageCounter.get());

        Map<String, Long> messagesByType = new HashMap<>();
        typeCounters.forEach((type, counter) ->
                messagesByType.put(type, counter.get())
        );
        status.put("messagesByType", messagesByType);

        return HttpResponse.ok(status);
    }

    @Post("/reset-counters")
    public HttpResponse<String> resetCounters() {
        messageCounter.set(0);
        typeCounters.clear();
        return HttpResponse.ok("Counters reset successfully");
    }
}

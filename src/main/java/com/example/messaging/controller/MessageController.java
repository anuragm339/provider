package com.example.messaging.controller;

import com.example.messaging.core.pipeline.service.PipelineManager;
import com.example.messaging.models.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller("/api/messages")
public class MessageController {
    private static final Logger logger = LoggerFactory.getLogger(MessageController.class);
    private final PipelineManager pipelineManager;

    @Inject
    public MessageController(PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
    }

    @Post("/single")
    public HttpResponse<String> submitMessage(@Body String request1) {
        try {
            MessageRequest request = new ObjectMapper().readValue(request1, MessageRequest.class);
            Message message = Message.builder()
                    .msgOffset(request.getOffset())
                    .type(request.getType())
                    .createdUtc(Instant.now())
                    .data(request.getData().getBytes())
                    .build();

            pipelineManager.submitMessage(message)
                    .thenAccept(offset ->
                            logger.debug("Message processed with offset: {}", offset))
                    .exceptionally(throwable -> {
                        logger.error("Failed to process message", throwable);
                        return null;
                    });

            return HttpResponse.ok("Message submitted successfully");
        } catch (Exception e) {
            logger.error("Error submitting message", e);
            return HttpResponse.serverError("Failed to submit message");
        }
    }

    @Post("/batch")
    public HttpResponse<String> submitBatch(@Body List<MessageRequest> requests) {
        try {
            List<Message> messages = requests.stream()
                    .map(req -> Message.builder()
                            .msgOffset(req.getOffset())
                            .type(req.getType())
                            .createdUtc(Instant.now())
                            .data(req.getData().getBytes())
                            .build())
                    .toList();

            pipelineManager.submitBatch(messages)
                    .thenAccept(offsets ->
                            logger.debug("Batch processed with offsets: {}", offsets))
                    .exceptionally(throwable -> {
                        logger.error("Failed to process batch", throwable);
                        return null;
                    });

            return HttpResponse.ok("Batch submitted successfully");
        } catch (Exception e) {
            logger.error("Error submitting batch", e);
            return HttpResponse.serverError("Failed to submit batch");
        }
    }
}

// Request DTO
class MessageRequest {
    private long offset;
    private String type;
    private String data;

    // Getters and setters
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getData() { return data; }
    public void setData(String data) { this.data = data; }
}

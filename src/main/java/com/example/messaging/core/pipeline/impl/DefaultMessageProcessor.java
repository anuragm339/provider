package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.MessageProcessor;
import com.example.messaging.core.validation.service.MessageValidator;
import com.example.messaging.core.pipeline.service.PipelineConfig;
import com.example.messaging.core.pipeline.service.ProcessingResult;

import com.example.messaging.exceptions.ErrorCode;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ResourceExhaustedException;
import com.example.messaging.exceptions.ValidationException;
import com.example.messaging.exceptions.VerificationException;
import com.example.messaging.models.Message;

import com.example.messaging.storage.db.rocks.duplicate.BidirectionalDuplicateHandler;
import com.example.messaging.storage.service.MessageStore;
import com.example.messaging.transport.rsocket.consumer.ConsumerConnection;
import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.handler.MessagePublisher;
import com.example.messaging.transport.rsocket.model.TransportMessage;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Base64;
import java.util.Optional;

@Singleton
public class DefaultMessageProcessor implements MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageProcessor.class);
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private final MessageValidator validator;
    private final PipelineConfig config;
    private final Executor executor;
    private final MessageStore messageStore;
    private final AtomicInteger activeProcessingCount;
    private final AtomicBoolean operational;
    private final MessagePublisher messagePublisher;
    private final ConsumerRegistry consumerRegistry;
    private final BidirectionalDuplicateHandler bidirectionalDuplicateHandler;

    public DefaultMessageProcessor(
            MessageValidator validator,
            PipelineConfig config,
            Executor executor,
            MessageStore messageStore,
            MessagePublisher messagePublisher,
            ConsumerRegistry consumerRegistry,
            BidirectionalDuplicateHandler bidirectionalDuplicateHandler) {
        this.validator = validator;
        this.config = config;
        this.executor = executor;
        this.messageStore = messageStore;
        this.activeProcessingCount = new AtomicInteger(0);
        this.operational = new AtomicBoolean(true);
        this.messagePublisher = messagePublisher;
        this.consumerRegistry=consumerRegistry;
        this.bidirectionalDuplicateHandler=bidirectionalDuplicateHandler;
    }



    private ProcessingResult processWithRetry(Message message) {
        Exception lastException = null;
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                return doProcessMessage(message);
            } catch (ValidationException e) {
                // Don't retry validation failures
                throw e;
            } catch (Exception e) {
                lastException = e;
                if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                    logger.warn("Retry attempt {} for message {}", attempt + 1, message.getMsgOffset());
                    sleep(RETRY_DELAY_MS * (attempt + 1));
                }
            }
        }
        throw new ProcessingException(
                "Failed after " + MAX_RETRY_ATTEMPTS + " attempts",
                ErrorCode.RETRY_LIMIT_EXCEEDED.getCode(),
                false,
                lastException
        );
    }

    private ProcessingResult doProcessMessage(Message message) {
        try {
            // Step 1: Validate message
            validator.validate(message);

            // Step 2: Calculate checksum for data integrity
            String checksum = calculateChecksum(message);

            // Step 3: Process message (actual business logic would go here)
            processMessageContent(message);

            // Step 4: Create successful result
            return new DefaultProcessingResult.Builder()
                    .offset(message.getMsgOffset())
                    .checksum(checksum)
                    .successful(true)
                    .processingTimestamp(Instant.now().toEpochMilli())
                    .build();

        } catch (Exception e) {
            logger.error("Error processing message: {}", message.getMsgOffset(), e);
            throw new ProcessingException(
                    "Failed to process message",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    private void processMessageContent(Message message) {
        //store the message in the duplicate handler
       // bidirectionalDuplicateHandler.checkAndUpdate(message);
        // Placeholder for actual message processing logic
        // This would be implemented based on business requirements
        messagePublisher.publishMessage(message, "group-1")
                .doOnSubscribe(__ -> logger.debug("Starting publish for message {}", message.getMsgOffset()))
                .doOnSuccess(__ -> logger.debug("Message {} published successfully", message.getMsgOffset()))
                .doOnError(error -> logger.error("Failed to publish message {}: {}",
                        message.getMsgOffset(), error.getMessage()))
                .block();
        logger.debug("Message {} processing complete", message.getMsgOffset());
    }

    private ProcessingResult createFailureResult(Message message, Exception e) {
        try {
            return new DefaultProcessingResult.Builder()
                    .offset(message.getMsgOffset())
                    .checksum(calculateChecksum(message))
                    .successful(false)
                    .processingTimestamp(Instant.now().toEpochMilli())
                    .build();
        } catch (Exception ex) {
            logger.error("Failed to create failure result", ex);
            throw new ProcessingException(
                    "Failed to create failure result",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    ex
            );
        }
    }

    @Override
    public CompletableFuture<ProcessingResult> processMessage(Message message) {
        if (!canAccept()) {
            logger.debug("MessageProcessor cannot accept more messages");
            return CompletableFuture.failedFuture(
                    new ResourceExhaustedException(
                            "Processor cannot accept more messages",
                            ErrorCode.QUEUE_FULL.getCode()
                    )
            );
        }

        return CompletableFuture.supplyAsync(() -> {
            ProcessingResult result = null;
            try {
                logger.debug("MessageProcessor: Starting processing for message: {}" , message.getMsgOffset());
                activeProcessingCount.incrementAndGet();

                logger.debug("MessageProcessor: Validating message: {}" , message.getMsgOffset());
                validator.validate(message);

                logger.debug("MessageProcessor: Processing with retry: {}" , message.getMsgOffset());
                result = processWithRetry(message);

                logger.debug("MessageProcessor: Starting to store result in database for offset: {}" , result.getOffset());
                storeResult(result);
                logger.debug("MessageProcessor: Successfully stored result in database for offset: {}" , result.getOffset());

                logger.debug("MessageProcessor: Processing complete for: {}" , message.getMsgOffset());
                return result;

            } catch (Exception e) {
                logger.error("MessageProcessor: Failed to process message:  {} " , message.getMsgOffset());
                e.printStackTrace();
                result = createFailureResult(message, e);
                storeResult(result);
                throw new ProcessingException(
                        "Message processing failed",
                        ErrorCode.PROCESSING_FAILED.getCode(),
                        true,
                        e
                );
            } finally {
                activeProcessingCount.decrementAndGet();
                logProcessingComplete(message, result);
            }
        }, executor);
    }

    private void storeResult(ProcessingResult result) {
        try {
            logger.debug("Starting to store result in database for offset: {}" , result.getOffset());
            messageStore.storeProcessingResult(result).join();
            logger.debug("Successfully stored result in database for offset:{}  " , result.getOffset());
        } catch (Exception e) {
            logger.debug("Failed to store processing result for offset: {}" , result.getOffset());
            e.printStackTrace();
            throw new ProcessingException(
                    "Failed to store processing result",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    @Override
    public boolean verifyProcessing(long messageOffset, String integrityToken) {
        try {
            // Step 1: Retrieve the original message and its processing result
            Message message = messageStore.getMessage(messageOffset)
                    .thenApply(this::validateMessagePresent)
                    .join();

            ProcessingResult storedResult = messageStore.getProcessingResult(messageOffset)
                    .thenApply(this::validateResultPresent)
                    .join();

            // Step 2: Verify the integrity token matches
            if (!storedResult.getIntegrityToken().equals(integrityToken)) {
                logger.error("Integrity token mismatch for message: {}", messageOffset);
                throw new VerificationException(
                        "Integrity token mismatch",
                        ErrorCode.INTEGRITY_CHECK_FAILED.getCode()
                );
            }

            // Step 3: Recalculate checksum and verify
            String recalculatedChecksum = calculateChecksum(message);
            if (!storedResult.getChecksum().equals(recalculatedChecksum)) {
                logger.error("Checksum mismatch for message: {}", messageOffset);
                throw new VerificationException(
                        "Checksum mismatch",
                        ErrorCode.CHECKSUM_MISMATCH.getCode()
                );
            }

            // Step 4: Verify processing was successful
            if (!storedResult.isSuccessful()) {
                logger.error("Processing was not successful for message: {}", messageOffset);
                return false;
            }

            logger.debug("Successfully verified message processing: {}", messageOffset);
            return true;

        } catch (Exception e) {
            logger.error("Verification failed for message: {}", messageOffset, e);
            return false;
        }
    }

    private Message validateMessagePresent(Optional<Message> messageOpt) {
        return messageOpt.orElseThrow(() ->
                new ProcessingException(
                        "Message not found",
                        ErrorCode.MISSING_VERIFICATION_DATA.getCode()
                )
        );
    }

    private ProcessingResult validateResultPresent(Optional<ProcessingResult> resultOpt) {
        return resultOpt.orElseThrow(() ->
                new ProcessingException(
                        "Processing result not found",
                        ErrorCode.MISSING_VERIFICATION_DATA.getCode()
                )
        );
    }

    private String calculateChecksum(Message message) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Include all relevant message fields in checksum
            digest.update(Long.toString(message.getMsgOffset()).getBytes(StandardCharsets.UTF_8));
            digest.update(message.getType().getBytes(StandardCharsets.UTF_8));
            digest.update(message.getData());

            byte[] hash = digest.digest();
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new ProcessingException(
                    "Failed to calculate checksum",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    @Override
    public boolean canAccept() {
        return isOperational() &&
                activeProcessingCount.get() < config.getMaxConcurrentMessages();
    }

    @Override
    public boolean isOperational() {
        return operational.get() && validator.isOperational();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessingException(
                    "Processing interrupted",
                    ErrorCode.PROCESSING_FAILED.getCode(),
                    true,
                    e
            );
        }
    }

    private void logProcessingComplete(Message message, ProcessingResult result) {
        if (result != null) {
            logger.debug("Message processing completed - Offset: {}, Success: {}",
                    message.getMsgOffset(), result.isSuccessful());
        }
    }

    // Control methods
    public void shutdown() {
        logger.debug("Shutting down message processor");
        operational.set(false);
    }

    public void start() {
        logger.debug("Starting message processor");
        operational.set(true);
    }

    // Metrics
    public int getCurrentProcessingCount() {
        return activeProcessingCount.get();
    }
}

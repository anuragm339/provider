package com.example.messaging.core.validation.impl;

import com.example.messaging.core.validation.service.MessageValidator;
import com.example.messaging.models.Message;
import com.example.messaging.exceptions.ValidationException;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import static com.example.messaging.constants.MessageConstants.MAX_MESSAGE_SIZE_BYTES;

@Singleton
public class DefaultMessageValidator implements MessageValidator {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageValidator.class);

    @Override
    public void validate(Message message) {
        if (message == null) {
            throw new ValidationException("Message cannot be null");
        }

        // Validate type
        if (message.getType() == null || message.getType().trim().isEmpty()) {
            throw new ValidationException("Message type cannot be null or empty");
        }

        // Validate data
        validateData(message.getData());

        // Validate timestamp
        validateTimestamp(message.getCreatedUtc());

        logger.debug("Message validation successful for type: {}", message.getType());
    }

    private void validateData(byte[] data) {
        if (data == null) {
            throw new ValidationException("Message data cannot be null");
        }

        if (data.length > MAX_MESSAGE_SIZE_BYTES) {
            throw new ValidationException(
                    String.format("Message size %d exceeds maximum allowed size %d",
                            data.length, MAX_MESSAGE_SIZE_BYTES)
            );
        }
    }

    private void validateTimestamp(Instant timestamp) {
        if (timestamp == null) {
            throw new ValidationException("Message timestamp cannot be null");
        }

        Instant now = Instant.now();
        // Allow messages with timestamps up to 1 hour in the future (clock skew)
        if (timestamp.isAfter(now.plusSeconds(3600))) {
            throw new ValidationException("Message timestamp too far in the future");
        }

        // Reject messages older than 24 hours
        if (timestamp.isBefore(now.minusSeconds(86400))) {
            throw new ValidationException("Message timestamp too old");
        }
    }
}

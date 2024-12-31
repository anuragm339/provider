package com.example.messaging.core.validation.service;

import com.example.messaging.models.Message;
import com.example.messaging.exceptions.ValidationException;

/**
 * Interface for message validation in the pipeline
 */
public interface MessageValidator {

    /**
     * Validates a message according to defined rules
     *
     * @param message The message to validate
     * @throws ValidationException if the message fails validation
     */
    void validate(Message message) throws ValidationException;

    /**
     * Checks if validator is operational
     *
     * @return true if validator is ready to validate messages
     */
    default boolean isOperational() {
        return true;
    }
}

package com.example.messaging.exceptions;

public class ValidationException extends MessageProcessingException {

    public ValidationException(String message) {
        super(message, "VAL_001", false); // Validation errors are not retryable
    }

    public ValidationException(String message, String errorCode) {
        super(message, errorCode, false);
    }

    public ValidationException(String message, String errorCode, Throwable cause) {
        super(message, errorCode, false, cause);
    }
}

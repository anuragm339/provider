package com.example.messaging.exceptions;

public class ResourceExhaustedException extends MessageProcessingException {

    public ResourceExhaustedException(String message) {
        super(message, "RES_001", true); // Resource issues are typically retryable
    }

    public ResourceExhaustedException(String message, String errorCode) {
        super(message, errorCode, true);
    }

    public ResourceExhaustedException(String message, String errorCode, Throwable cause) {
        super(message, errorCode, true, cause);
    }
}

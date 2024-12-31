package com.example.messaging.exceptions;

public class ProcessingException extends MessageProcessingException {

    public ProcessingException(String message) {
        super(message, "PRO_001", true); // General processing errors are retryable by default
    }

    public ProcessingException(String message, String errorCode) {
        super(message, errorCode, true);
    }

    public ProcessingException(String message, String errorCode, boolean retryable) {
        super(message, errorCode, retryable);
    }

    public ProcessingException(String message, String errorCode, boolean retryable, Throwable cause) {
        super(message, errorCode, retryable, cause);
    }
}

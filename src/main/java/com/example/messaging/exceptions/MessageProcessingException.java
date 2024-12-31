package com.example.messaging.exceptions;

/**
 * Base exception for all message processing related exceptions
 */
public class MessageProcessingException extends RuntimeException {
    private final String errorCode;
    private final boolean retryable;

    public MessageProcessingException(String message, String errorCode, boolean retryable) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    public MessageProcessingException(String message, String errorCode, boolean retryable, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}

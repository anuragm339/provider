package com.example.messaging.exceptions;

public class VerificationException extends MessageProcessingException {

    public VerificationException(String message) {
        super(message, "VER_001", true); // Verification failures might be retryable
    }

    public VerificationException(String message, String errorCode) {
        super(message, errorCode, true);
    }

    public VerificationException(String message, String errorCode, Throwable cause) {
        super(message, errorCode, true, cause);
    }
}

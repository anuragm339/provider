package com.example.messaging.exceptions;

public class TransportException extends MessageProcessingException {

    public TransportException(String message) {
        super(message, ErrorCode.TRANSPORT_FAILED.getCode(), true);
    }

    public TransportException(String message, String errorCode) {
        super(message, errorCode, true);
    }

    public TransportException(String message, String errorCode, Throwable cause) {
        super(message, errorCode, true, cause);
    }
}

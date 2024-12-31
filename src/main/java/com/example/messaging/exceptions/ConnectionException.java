package com.example.messaging.exceptions;

public class ConnectionException extends TransportException {

    public ConnectionException(String message) {
        super(message, ErrorCode.CONNECTION_FAILED.getCode());
    }

    public ConnectionException(String message, String errorCode) {
        super(message, errorCode);
    }

    public ConnectionException(String message, String errorCode, Throwable cause) {
        super(message, errorCode, cause);
    }
}

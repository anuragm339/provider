package com.example.messaging.exceptions;

public enum ErrorCode {
    // Resource errors (RES_XXX)
    QUEUE_FULL("RES_001", "Processing queue is full"),
    MEMORY_EXHAUSTED("RES_002", "System memory exhausted"),
    THREAD_POOL_EXHAUSTED("RES_003", "Thread pool exhausted"),
    SYSTEM_OVERLOAD("RES_004", "System processing capacity exceeded"),

    // Processing errors (PRO_XXX)
    PROCESSING_FAILED("PRO_001", "Message processing failed"),
    TIMEOUT("PRO_002", "Processing timeout"),
    RETRY_LIMIT_EXCEEDED("PRO_003", "Retry limit exceeded"),
    PARTIAL_BATCH_FAILURE("PRO_004", "Partial batch processing failure"),

    // Validation errors (VAL_XXX)
    INVALID_MESSAGE("VAL_001", "Invalid message format"),
    MISSING_REQUIRED_FIELD("VAL_002", "Required field is missing"),
    INVALID_DATA_SIZE("VAL_003", "Data size exceeds limit"),
    INVALID_MESSAGE_SEQUENCE("VAL_004", "Invalid message sequence"),
    DUPLICATE_MESSAGE("VAL_005", "Duplicate message detected"),

    // Batch errors (BAT_XXX)
    BATCH_STATE_INVALID("BAT_001", "Invalid batch state"),
    BATCH_EXPIRED("BAT_002", "Batch has expired"),
    BATCH_SIZE_INVALID("BAT_003", "Invalid batch size"),
    BATCH_SEQUENCE_ERROR("BAT_004", "Batch sequence error"),
    BATCH_PROCESSING_ERROR("BAT_005", "Batch processing error"),

    INTEGRITY_CHECK_FAILED("VER_001", "Message integrity check failed"),
    CHECKSUM_MISMATCH("VER_002", "Message checksum mismatch"),
    MISSING_VERIFICATION_DATA("VER_003", "Verification data not found"),
    VERIFICATION_TIMEOUT("VER_004", "Verification process timed out"),
    DATA_CORRUPTION("VER_005", "Message data corruption detected"),


    // Transport errors (TRA_XXX)
    TRANSPORT_FAILED("TRA_001", "Transport operation failed"),
    CONNECTION_FAILED("TRA_002", "Connection operation failed"),
    CONSUMER_NOT_FOUND("TRA_003", "Consumer not found"),
    ENCODING_FAILED("TRA_004", "Message encoding failed"),
    DECODING_FAILED("TRA_005", "Message decoding failed"),
    CONSUMER_DISCONNECTED("TRA_006", "Consumer disconnected"),
    REPLAY_FAILED("TRA_007", "Message replay failed"),
    NO_ACTIVE_CONSUMERS("TRA_008", "No active consumers in group"),;

    private final String code;
    private final String description;

    ErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static ErrorCode fromCode(String code) {
        for (ErrorCode errorCode : values()) {
            if (errorCode.code.equals(code)) {
                return errorCode;
            }
        }
        throw new IllegalArgumentException("Unknown error code: " + code);
    }
}

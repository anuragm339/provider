package com.example.messaging.storage.db.rocks.model;


import java.util.Arrays;

public class DuplicateResult {
    private final boolean isDuplicate;
    private final byte[] existingValue;

    public DuplicateResult(boolean isDuplicate, byte[] existingValue) {
        this.isDuplicate = isDuplicate;
        this.existingValue = existingValue != null ? Arrays.copyOf(existingValue, existingValue.length) : null;
    }

    public boolean isDuplicate() {
        return isDuplicate;
    }

    public byte[] getExistingValue() {
        return existingValue != null ? Arrays.copyOf(existingValue, existingValue.length) : null;
    }
}


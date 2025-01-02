package com.example.messaging.storage.db.rocks.model;

import java.util.concurrent.atomic.AtomicInteger;

public class  DeletionStatus {
    private volatile boolean completed;
    private final AtomicInteger retryCount = new AtomicInteger(0);

    public void incrementRetryCount() {
        retryCount.incrementAndGet();
    }
}

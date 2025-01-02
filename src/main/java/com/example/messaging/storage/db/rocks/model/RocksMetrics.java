package com.example.messaging.storage.db.rocks.model;


public record RocksMetrics(
        long keysWritten,
        long keysRead,
        long bytesWritten,
        long bytesRead,
        long writeErrors,
        long readErrors
) {}

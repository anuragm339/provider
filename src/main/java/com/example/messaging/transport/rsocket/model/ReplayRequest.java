package com.example.messaging.transport.rsocket.model;

import com.example.messaging.transport.rsocket.protocol.RequestType;

public class ReplayRequest extends ConsumerRequest {
    private final long fromOffset;
    private final long toOffset;
    private final int batchSize;

    public ReplayRequest(String consumerId, long fromOffset, long toOffset, int batchSize) {
        super(consumerId, RequestType.REPLAY, null);
        this.fromOffset = fromOffset;
        this.toOffset = toOffset;
        this.batchSize = batchSize;
    }

    // Getters
    public long getFromOffset() { return fromOffset; }
    public long getToOffset() { return toOffset; }
    public int getBatchSize() { return batchSize; }
}

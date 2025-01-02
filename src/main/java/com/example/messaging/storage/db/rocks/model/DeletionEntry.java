package com.example.messaging.storage.db.rocks.model;

public   class DeletionEntry {
    final byte[] key;
    final long offset;

    public DeletionEntry(byte[] key, long offset) {
        this.key = key;
        this.offset = offset;
    }

    public byte[] getKey() {
        return key;
    }

    public long getOffset() {
        return offset;
    }
}

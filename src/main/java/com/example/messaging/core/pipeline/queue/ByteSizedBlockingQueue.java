package com.example.messaging.core.pipeline.queue;

import com.example.messaging.models.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class ByteSizedBlockingQueue {
    private static final Logger logger = LoggerFactory.getLogger(ByteSizedBlockingQueue.class);

    private final LinkedBlockingQueue<Message> queue;
    private final AtomicLong currentSizeBytes;
    private final long maxSizeBytes;
    private final ReentrantLock sizeLock;
    private final String consumerId;

    public ByteSizedBlockingQueue(String consumerId, int initialCapacity, long maxSizeBytes) {
        this.queue = new LinkedBlockingQueue<>(initialCapacity);
        this.currentSizeBytes = new AtomicLong(0);
        this.maxSizeBytes = maxSizeBytes;
        this.sizeLock = new ReentrantLock();
        this.consumerId = consumerId;
    }

    public boolean contains(Message message) {
        return queue.contains(message);
    }

    public boolean offer(Message message, long timeout, TimeUnit unit) throws InterruptedException {
        long messageSize = calculateMessageSize(message);

        sizeLock.lock();
        try {
            if (currentSizeBytes.get() + messageSize > maxSizeBytes) {
                logger.warn("Queue size limit exceeded for consumer {}. Current: {} bytes, Max: {} bytes, Message size: {} bytes",
                        consumerId, currentSizeBytes.get(), maxSizeBytes, messageSize);
                return false;
            }

            if (queue.offer(message, timeout, unit)) {
                currentSizeBytes.addAndGet(messageSize);
                logger.info("Added message {} to queue for consumer {}. New size: {} bytes",
                        message.getMsgOffset(), consumerId, currentSizeBytes.get());
                return true;
            }
            logger.warn("Failed to add message {} to queue for consumer {} within timeout",
                    message.getMsgOffset(), consumerId);
            return false;
        } finally {
            sizeLock.unlock();
        }
    }

    public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
        Message message = queue.poll(timeout, unit);
        if (message != null) {
            long messageSize = calculateMessageSize(message);
            currentSizeBytes.addAndGet(-messageSize);
            logger.debug("Removed message from queue for consumer {}. New size: {} bytes",
                    consumerId, currentSizeBytes.get());
        }
        return message;
    }

    public Message poll() {
        Message message = queue.poll();
        if (message != null) {
            long messageSize = calculateMessageSize(message);
            currentSizeBytes.addAndGet(-messageSize);
        }
        return message;
    }

    private long calculateMessageSize(Message message) {
        // Base size for message metadata
        long size = 8 + // msgOffset (long)
                4 + // type string reference
                8 + // createdUtc (Instant)
                4;  // state (enum)

        // Add actual data size
        if (message.getData() != null) {
            size += message.getData().length;
        }

        // Add type string size if present
        if (message.getType() != null) {
            size += message.getType().length() * 2; // Unicode characters
        }

        return size;
    }

    public long getCurrentSizeBytes() {
        return currentSizeBytes.get();
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public void clear() {
        sizeLock.lock();
        try {
            queue.clear();
            currentSizeBytes.set(0);
        } finally {
            sizeLock.unlock();
        }
    }

    public double getUsagePercentage() {
        return (double) currentSizeBytes.get() / maxSizeBytes * 100;
    }

    public long getRemainingCapacityBytes() {
        return maxSizeBytes - currentSizeBytes.get();
    }
}

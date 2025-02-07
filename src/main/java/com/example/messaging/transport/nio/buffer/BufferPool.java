package com.example.messaging.transport.nio.buffer;

import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class BufferPool {
    private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);

    private final Queue<ByteBuffer> pool;
    private final int bufferSize;
    private final int maxPoolSize;
    private final AtomicInteger currentPoolSize;
    private final boolean isDirect;

    public BufferPool(
            @Value("${nio.buffer.size:8192}") int bufferSize,
            @Value("${nio.buffer.pool.size:1000}") int maxPoolSize,
            @Value("${nio.buffer.direct:true}") boolean isDirect) {
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
        this.isDirect = isDirect;
        this.pool = new ConcurrentLinkedQueue<>();
        this.currentPoolSize = new AtomicInteger(0);

        // Pre-allocate some buffers
        int initialSize = Math.min(100, maxPoolSize);
        for (int i = 0; i < initialSize; i++) {
            pool.offer(createBuffer());
            currentPoolSize.incrementAndGet();
        }

        logger.info("Buffer pool initialized with {} buffers of size {}",
                initialSize, bufferSize);
    }

    public ByteBuffer acquire() {
        ByteBuffer buffer = pool.poll();
        if (buffer == null) {
            if (currentPoolSize.get() < maxPoolSize) {
                buffer = createBuffer();
                currentPoolSize.incrementAndGet();
            } else {
                logger.warn("Buffer pool maximum size reached. Creating temporary buffer.");
                buffer = createBuffer();
            }
        }
        buffer.clear();
        return buffer;
    }

    public void release(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        if (currentPoolSize.get() < maxPoolSize) {
            buffer.clear();
            pool.offer(buffer);
        }
    }

    private ByteBuffer createBuffer() {
        return isDirect ? ByteBuffer.allocateDirect(bufferSize)
                : ByteBuffer.allocate(bufferSize);
    }

    public int getAvailableBuffers() {
        return pool.size();
    }

    public int getCurrentPoolSize() {
        return currentPoolSize.get();
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void clear() {
        pool.clear();
        currentPoolSize.set(0);
        logger.info("Buffer pool cleared");
    }
}

package com.example.messaging.transport.nio.connection;

import com.example.messaging.transport.nio.buffer.BufferPool;
import com.example.messaging.transport.nio.protocal.MessageFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ClientConnection {
    private static final Logger logger = LoggerFactory.getLogger(ClientConnection.class);

    private final String connectionId;
    private final SocketChannel channel;
    private final Selector selector;
    private final BufferPool bufferPool;
    private final Queue<ByteBuffer> writeQueue;
    private ByteBuffer partialReadBuffer;

    // Metrics
    private final AtomicLong bytesReceived;
    private final AtomicLong bytesSent;
    private final AtomicLong messagesReceived;
    private final AtomicLong messagesSent;
    private final Instant createdAt;
    private Instant lastActivityTime;

    // State
    private volatile boolean active;
    private volatile ConnectionState state;

    public enum ConnectionState {
        CONNECTING,
        AUTHENTICATING,
        ACTIVE,
        CLOSING,
        CLOSED
    }

    public ClientConnection(String connectionId, SocketChannel channel, Selector selector, BufferPool bufferPool) {
        this.connectionId = connectionId;
        this.channel = channel;
        this.selector = selector;
        this.bufferPool = bufferPool;
        this.writeQueue = new ConcurrentLinkedQueue<>();
        this.bytesReceived = new AtomicLong(0);
        this.bytesSent = new AtomicLong(0);
        this.messagesReceived = new AtomicLong(0);
        this.messagesSent = new AtomicLong(0);
        this.createdAt = Instant.now();
        this.lastActivityTime = Instant.now();
        this.active = true;
        this.state = ConnectionState.CONNECTING;
    }

    public void processIncomingData(ByteBuffer data) {
        if (!active) {
            return;
        }

        try {
            updateLastActivityTime();
            int received = data.remaining();
            bytesReceived.addAndGet(received);

            // Handle partial message from previous read
            if (partialReadBuffer != null) {
                data = combineBuffers(partialReadBuffer, data);
                partialReadBuffer = null;
            }

            // Process complete messages
            while (data.hasRemaining()) {
                if (!MessageFrame.hasCompleteMessage(data)) {
                    // Store partial message for next read
                    partialReadBuffer = ByteBuffer.allocate(data.remaining());
                    partialReadBuffer.put(data);
                    partialReadBuffer.flip();
                    break;
                }

                MessageFrame frame = MessageFrame.fromBuffer(data);
                if (frame != null) {
                    handleMessage(frame);
                    messagesReceived.incrementAndGet();
                }
            }

        } catch (Exception e) {
            logger.error("Error processing incoming data for connection: {}", connectionId, e);
            close();
        }
    }

    public void queueMessage(ByteBuffer message) {
        if (!active) {
            return;
        }

        try {
            // Create message frame
            ByteBuffer framedMessage = MessageFrame.wrapMessage(message);
            writeQueue.offer(framedMessage);
            messagesSent.incrementAndGet();

            // Update channel write interest
            SelectionKey key = channel.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        } catch (Exception e) {
            logger.error("Error queuing message for connection: {}", connectionId, e);
        }
    }

    public void writeOutgoingData() throws IOException {
        if (!active) {
            return;
        }

        ByteBuffer current = null;
        try {
            while ((current = writeQueue.peek()) != null) {
                int written = channel.write(current);
                if (written < 0) {
                    throw new IOException("Connection closed by peer");
                }
                bytesSent.addAndGet(written);
                updateLastActivityTime();

                if (current.hasRemaining()) {
                    // Partial write - leave in queue and wait for next write opportunity
                    break;
                }
                writeQueue.poll(); // Remove completely written buffer
            }
        } catch (IOException e) {
            logger.error("Error writing data for connection: {}", connectionId, e);
            close();
            throw e;
        }
    }

    public void close() {
        if (!active) {
            return;
        }

        active = false;
        state = ConnectionState.CLOSING;

        try {
            channel.close();
        } catch (IOException e) {
            logger.error("Error closing channel for connection: {}", connectionId, e);
        } finally {
            state = ConnectionState.CLOSED;
            clearBuffers();
        }
    }

    private void clearBuffers() {
        writeQueue.clear();
        if (partialReadBuffer != null) {
            partialReadBuffer = null;
        }
    }

    private ByteBuffer combineBuffers(ByteBuffer old, ByteBuffer fresh) {
        ByteBuffer combined = ByteBuffer.allocate(old.remaining() + fresh.remaining());
        combined.put(old);
        combined.put(fresh);
        combined.flip();
        return combined;
    }

    private void handleMessage(MessageFrame frame) {
        // TODO: Implement message handling logic
        logger.debug("Received message of type: {} for connection: {}",
                frame.getType(), connectionId);
    }

    private void updateLastActivityTime() {
        this.lastActivityTime = Instant.now();
    }

    // Getters for metrics and state
    public long getBytesReceived() { return bytesReceived.get(); }
    public long getBytesSent() { return bytesSent.get(); }
    public long getMessagesReceived() { return messagesReceived.get(); }
    public long getMessagesSent() { return messagesSent.get(); }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getLastActivityTime() { return lastActivityTime; }
    public boolean isActive() { return active; }
    public ConnectionState getState() { return state; }
    public String getConnectionId() { return connectionId; }
    public boolean hasDataToWrite() { return !writeQueue.isEmpty(); }
}

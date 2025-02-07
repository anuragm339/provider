package com.example.messaging.transport.nio.handler;

import com.example.messaging.transport.nio.connection.ClientConnection;
import com.example.messaging.transport.nio.buffer.BufferPool;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;

@Singleton
public class ConnectionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

    private final Map<SocketChannel, ClientConnection> connections;
    private final BufferPool bufferPool;
    private final Selector selector;
    private static final int BUFFER_SIZE = 8192; // 8KB default buffer size

    public ConnectionHandler(BufferPool bufferPool, Selector selector) {
        this.connections = new ConcurrentHashMap<>();
        this.bufferPool = bufferPool;
        this.selector = selector;
    }

    public void handleNewConnection(SocketChannel channel) {
        String connectionId = UUID.randomUUID().toString();
        ClientConnection connection = new ClientConnection(connectionId, channel, selector, bufferPool);
        connections.put(channel, connection);
        logger.debug("New connection registered: {}", channel);
    }

    public void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientConnection connection = connections.get(channel);

        if (connection == null) {
            logger.warn("No connection found for channel: {}", channel);
            key.cancel();
            channel.close();
            return;
        }

        ByteBuffer readBuffer = bufferPool.acquire();
        try {
            int bytesRead = channel.read(readBuffer);

            if (bytesRead == -1) {
                handleDisconnect(channel);
                return;
            }

            if (bytesRead > 0) {
                readBuffer.flip();
                connection.processIncomingData(readBuffer);
            }
        } finally {
            bufferPool.release(readBuffer);
        }
    }

    public void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientConnection connection = connections.get(channel);

        if (connection == null) {
            logger.warn("No connection found for channel: {}", channel);
            key.cancel();
            channel.close();
            return;
        }

        connection.writeOutgoingData();

        // If no more data to write, remove write interest
        if (!connection.hasDataToWrite()) {
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    public void handleDisconnect(SocketChannel channel) {
        try {
            ClientConnection connection = connections.remove(channel);
            if (connection != null) {
                connection.close();
                logger.debug("Connection closed and cleanup completed: {}", channel);
            }
            channel.close();
        } catch (IOException e) {
            logger.error("Error during disconnect handling", e);
        }
    }

    public void broadcast(ByteBuffer message) {
        connections.values().forEach(connection -> connection.queueMessage(message.duplicate()));
    }

    public void sendToChannel(SocketChannel channel, ByteBuffer message) {
        ClientConnection connection = connections.get(channel);
        if (connection != null) {
            connection.queueMessage(message.duplicate());
        }
    }

    public int getActiveConnectionCount() {
        return connections.size();
    }
}

package com.example.messaging.transport.nio.server;

import com.example.messaging.transport.nio.handler.ConnectionHandler;
import com.example.messaging.exceptions.ProcessingException;
import com.example.messaging.exceptions.ErrorCode;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
@Context
public class NIOServer {
    private static final Logger logger = LoggerFactory.getLogger(NIOServer.class);

    private final int port;
    private final ConnectionHandler connectionHandler;
    private final ExecutorService executorService;
    private final AtomicBoolean running;
    private Selector selector;
    private ServerSocketChannel serverChannel;

    public NIOServer(
            @Value("${nio.server.port:7100}") int port,
            ConnectionHandler connectionHandler) {
        this.port = port;
        this.connectionHandler = connectionHandler;
        this.executorService = Executors.newSingleThreadExecutor();
        this.running = new AtomicBoolean(false);
    }

    @PostConstruct
    public void init() {
        start();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            try {
                initializeServer();
                startEventLoop();
            } catch (IOException e) {
                running.set(false);
                throw new ProcessingException(
                        "Failed to start NIO server",
                        ErrorCode.CONNECTION_FAILED.getCode(),
                        false,
                        e
                );
            }
        }
    }

    private void initializeServer() throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        logger.info("NIO Server initialized on port {}", port);
    }

    private void startEventLoop() {
        executorService.submit(() -> {
            try {
                while (running.get()) {
                    if (selector.select() > 0) {
                        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                        while (keys.hasNext()) {
                            SelectionKey key = keys.next();
                            keys.remove();

                            if (!key.isValid()) {
                                continue;
                            }

                            try {
                                if (key.isAcceptable()) {
                                    handleAccept();
                                } else if (key.isReadable()) {
                                    connectionHandler.handleRead(key);
                                } else if (key.isWritable()) {
                                    connectionHandler.handleWrite(key);
                                }
                            } catch (Exception e) {
                                logger.error("Error handling channel event", e);
                                key.cancel();
                                try {
                                    key.channel().close();
                                } catch (IOException ce) {
                                    logger.error("Error closing channel", ce);
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                if (running.get()) {
                    logger.error("Error in event loop", e);
                }
            }
        });
    }

    private void handleAccept() throws IOException {
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);
            connectionHandler.handleNewConnection(clientChannel);
            logger.debug("Accepted new connection from: {}",
                    clientChannel.getRemoteAddress());
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                if (selector != null) {
                    selector.close();
                }
                if (serverChannel != null) {
                    serverChannel.close();
                }
                executorService.shutdown();
                logger.info("NIO Server stopped");
            } catch (IOException e) {
                logger.error("Error stopping server", e);
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }
}

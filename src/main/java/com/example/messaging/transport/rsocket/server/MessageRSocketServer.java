package com.example.messaging.transport.rsocket.server;

import com.example.messaging.transport.rsocket.config.RSocketConfiguration;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class MessageRSocketServer {
    private static final Logger logger = LoggerFactory.getLogger(MessageRSocketServer.class);

    private final RSocketConfiguration config;
    private final ConnectionAcceptor connectionAcceptor;
    private io.rsocket.transport.netty.server.CloseableChannel server;

    @Inject
    public MessageRSocketServer(
            RSocketConfiguration config,
            ConnectionAcceptor connectionAcceptor) {
        this.config = config;
        this.connectionAcceptor = connectionAcceptor;
    }

    public Mono<Void> start() {
        return RSocketServer.create()
                .acceptor(connectionAcceptor)
                .bind(TcpServerTransport.create(config.getPort()))
                .doOnSuccess(closeable -> {
                    this.server = closeable;
                    logger.info("RSocket server successfully bound to port " , config.getPort());
                })
                .doOnError(error ->{
                    logger.error("Failed to start RSocket server: " , error.getMessage());
        error.printStackTrace();
        })
                .then();
    }

    public Mono<Void> stop() {
        return Mono.fromRunnable(() -> {
            if (server != null) {
                server.dispose();
                logger.info("RSocket server stopped");
            }
        });
    }

    public boolean isRunning() {
        return server != null && !server.isDisposed();
    }
}

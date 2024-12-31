package com.example.messaging.transport.rsocket.health;

import com.example.messaging.transport.rsocket.consumer.ConsumerRegistry;
import com.example.messaging.transport.rsocket.server.MessageRSocketServer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import reactor.core.publisher.Mono;

@Singleton
public class RSocketHealthCheck {
    private final MessageRSocketServer server;
    private final ConsumerRegistry consumerRegistry;

    @Inject
    public RSocketHealthCheck(MessageRSocketServer server, ConsumerRegistry consumerRegistry) {
        this.server = server;
        this.consumerRegistry = consumerRegistry;
    }

    public Mono<HealthStatus> checkHealth() {
        return Mono.fromSupplier(() -> {
            HealthStatus status = new HealthStatus();

            // Check server status
            status.setServerRunning(server.isRunning());

            // Get consumer statistics
            long activeConsumers = consumerRegistry.getActiveConnections().count();
            status.setActiveConsumers(activeConsumers);

            return status;
        });
    }

    public static class HealthStatus {
        private boolean serverRunning;
        private long activeConsumers;

        public boolean isServerRunning() {
            return serverRunning;
        }

        public void setServerRunning(boolean serverRunning) {
            this.serverRunning = serverRunning;
        }

        public long getActiveConsumers() {
            return activeConsumers;
        }

        public void setActiveConsumers(long activeConsumers) {
            this.activeConsumers = activeConsumers;
        }
    }
}

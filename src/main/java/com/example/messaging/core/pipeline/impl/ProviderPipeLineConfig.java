package com.example.messaging.core.pipeline.impl;

import com.example.messaging.core.pipeline.service.PipelineConfig;
import jakarta.inject.Singleton;

@Singleton
public class ProviderPipeLineConfig implements PipelineConfig {
        @Override
        public int getMaxConcurrentMessages() {
            return 1000;
        }

        @Override
        public int getMaxQueueSize() {
            return 10000;
        }

        @Override
        public long getProcessingTimeoutMs() {
            return 5000;
        }

        @Override
        public boolean isRetryEnabled() {
            return true;
        }

        @Override
        public int getMaxRetryAttempts() {
            return 3;
        }

        @Override
        public long getRetryDelayMs() {
            return 1000;
        }

}

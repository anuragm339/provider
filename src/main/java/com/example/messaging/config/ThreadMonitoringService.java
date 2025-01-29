package com.example.messaging.config;

import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@Singleton
public class ThreadMonitoringService {
    private static final Logger logger = LoggerFactory.getLogger(ThreadMonitoringService.class);
    private final Map<String, ExecutorService> monitoredExecutors = new ConcurrentHashMap<>();

    public void registerExecutor(String name, ExecutorService executor) {
        monitoredExecutors.put(name, executor);
    }

    @Scheduled(fixedDelay = "1m")
    public void monitorThreads() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.getAllThreadIds();
        ThreadInfo[] threadInfo = threadBean.getThreadInfo(threadIds);

        Map<String, Integer> threadCountByGroup = new HashMap<>();

        for (ThreadInfo info : threadInfo) {
            if (info != null) {
                String groupName = info.getThreadName().split("-")[0];
                threadCountByGroup.merge(groupName, 1, Integer::sum);
            }
        }

        // Log thread counts by group
        threadCountByGroup.forEach((group, count) ->
                logger.info("Thread group {} count: {}", group, count));

        // Monitor executor metrics
        monitoredExecutors.forEach(this::logExecutorMetrics);
    }

    private void logExecutorMetrics(String name, ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
            logger.info("{} executor metrics - Active: {}, Pool: {}, Queue: {}, Completed: {}",
                    name,
                    tpe.getActiveCount(),
                    tpe.getPoolSize(),
                    tpe.getQueue().size(),
                    tpe.getCompletedTaskCount());
        }
    }
}

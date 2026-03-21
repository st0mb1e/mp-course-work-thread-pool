package ru.yartsev_vladislav;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class LoggingThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger counter = new AtomicInteger(1);

    public LoggingThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = poolName + "-worker-" + counter.getAndIncrement();

        Runnable factRunnable = () -> {
            try {
                runnable.run();
            } finally {
                System.out.println("[Worker] " + threadName + " terminated.");
            }
        };
        Thread thread = new Thread(factRunnable, threadName);

        System.out.println("[ThreadFactory] Creating new thread: " + threadName);

        return thread;
    }
}

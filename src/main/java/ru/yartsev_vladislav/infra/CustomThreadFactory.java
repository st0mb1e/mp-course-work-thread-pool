package ru.yartsev_vladislav.infra;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {
    private final String poolName;
    private final boolean debug;
    private final AtomicInteger counter = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this(poolName, true);
    }

    public CustomThreadFactory(String poolName, boolean debug) {
        this.poolName = poolName;
        this.debug = debug;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = poolName + "-worker-" + counter.getAndIncrement();

        Runnable factRunnable;
        if (debug) {
            factRunnable = () -> {
                try {
                    runnable.run();
                } finally {
                    System.out.println("[Worker] " + threadName + " terminated.");
                }
            };
        } else {
            factRunnable = runnable;
        }
        Thread thread = new Thread(factRunnable, threadName);

        if (debug) {
            System.out.println("[ThreadFactory] Creating new thread: " + threadName);
        }

        return thread;
    }
}

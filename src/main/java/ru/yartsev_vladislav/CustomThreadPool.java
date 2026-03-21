package ru.yartsev_vladislav;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CustomThreadPool implements CustomExecutor {
    private static final int NON_IDLE_WORKER_POLL_TIMEOUT_IN_SECONDS = 1;

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit unit;
    private final int queueSize;
    private final int minSpareThreads;
    private final ThreadFactory threadFactory;
    private final Set<Worker> workers = ConcurrentHashMap.newKeySet();

    private volatile Status status = Status.RUNNING;
    private CustomRejectedExecutionHandler rejectedExecutionHandler;

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, int queueSize, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.unit = unit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = new LoggingThreadFactory("testing");
        init();
    }

    public void setRejectedExecutionHandler(CustomRejectedExecutionHandler rejectedExecutionHandler) {
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }

    @Override
    public void execute(Runnable command) {
        pushTask(new FutureTask<>(command, null));
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        pushTask(future);
        return future;
    }

    @Override
    public void shutdown() {
        status = Status.SHUTDOWNED;
    }

    @Override
    public void shutdownNow() {
        status = Status.SHUTDOWNED_NOW;
        for (Worker w : workers) {
            w.stop();
        }
    }

    private void init() {
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(new Worker());
        }
        ensureSpareThreads();
    }

    private void addWorker(Worker worker) {
        workers.add(worker);
    }

    private void removeWorker(Worker worker) {
        synchronized (workers) {
            workers.remove(worker);
            ensureSpareThreads();
        }
    }

    private void ensureSpareThreads() {
        if (Status.SHUTDOWNED.equals(status) || Status.SHUTDOWNED_NOW.equals(status)) {
            return;
        }
        synchronized (workers) {
            int idleCount = 0;
            for (Worker w : workers) {
                if (w.isIdle()) {
                    idleCount++;
                }
            }

            int toCreate = Math.min(minSpareThreads - idleCount, maxPoolSize - workers.size());
            for (int i = 0; i < toCreate; i++) {
                Worker idle = new Worker(true);
                workers.add(idle);
            }
        }
    }


    private Worker getLeastLoadedWorker() {
        synchronized (workers) {
            return workers.stream().min(Comparator.comparingInt(Worker::getQueueSize)).get();
        }
    }

    private <T> void pushTask(FutureTask<T> task) {
        synchronized (workers) {
            Worker worker = getLeastLoadedWorker();

            try {
                if (Status.SHUTDOWNED.equals(status) || Status.SHUTDOWNED_NOW.equals(status)) {
                    throw new RejectedExecutionException("Rejected due to shutdown");
                }
                // Самый слабонагруженный воркер переполнен
                if (worker.getQueueSize() >= queueSize) {
                    throw new RejectedExecutionException("Rejected due to overload");
                }
                if (!worker.push(task)) {
                    throw new RejectedExecutionException("Rejected due to unknown error");
                }
            } catch (RejectedExecutionException e) {
                task.cancel(false);
                if (rejectedExecutionHandler != null) {
                    rejectedExecutionHandler.rejectedExecution(task, this, e);
                } else {
                    throw e;
                }
            }
        }
    }

    private enum Status {
        RUNNING,
        SHUTDOWNED,
        SHUTDOWNED_NOW,
    }

    private class Worker implements Runnable {
        private final boolean isIdle;
        private final BlockingQueue<FutureTask<?>> queue;
        private final Thread thread;

        public Worker() {
            this(false);
        }

        public Worker(boolean isIdle) {
            this.isIdle = isIdle;
            queue = new ArrayBlockingQueue<>(queueSize);
            // TODO: подумать над инкапсуляцией
            thread = threadFactory.newThread(this);
            thread.start();
        }

        public boolean push(FutureTask<?> futureTask) {
            return queue.offer(futureTask);
        }

        public boolean isIdle() {
            return isIdle;
        }

        public int getQueueSize() {
            return queue.size();
        }

        public void stop() {
            thread.interrupt();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (Status.SHUTDOWNED_NOW.equals(status) || thread.isInterrupted()) {
                        return;
                    }
                    if (Status.SHUTDOWNED.equals(status) && queue.isEmpty()) {
                        return;
                    }
                    FutureTask<?> futureTask;
                    if (isIdle) {
                        futureTask = queue.poll(keepAliveTime, unit);
                    } else {
                        futureTask = queue.poll(NON_IDLE_WORKER_POLL_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
                    }
                    if (futureTask == null) {
                        if (isIdle) {
                            return;
                        }

                        continue;
                    }
                    futureTask.run();
                }
            } catch (InterruptedException e) {
                // ignore
            } finally {
                removeWorker(this);
            }
        }
    }
}

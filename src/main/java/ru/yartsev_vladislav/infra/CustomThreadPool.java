package ru.yartsev_vladislav.infra;

import ru.yartsev_vladislav.port.CustomExecutor;
import ru.yartsev_vladislav.port.CustomRejectedExecutionHandler;

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
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPool implements CustomExecutor {
    private static final AtomicInteger poolCounter = new AtomicInteger(1);

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit unit;
    private final int queueSize;
    private final int minSpareThreads;
    private final boolean debug;
    private final String name;
    private final ThreadFactory threadFactory;
    private final Set<Worker> workers = ConcurrentHashMap.newKeySet();

    private volatile Status status = Status.RUNNING;
    private CustomRejectedExecutionHandler rejectedExecutionHandler;

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, int queueSize, int minSpareThreads) {
        this(corePoolSize, maxPoolSize, keepAliveTime, unit, queueSize, minSpareThreads, true);
    }

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, int queueSize, int minSpareThreads, boolean debug) {
        if (corePoolSize > maxPoolSize) {
            throw new IllegalArgumentException("corePoolSize is more than maxPoolSize");
        }
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.unit = unit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.debug = debug;
        name = "Custom-Thread-Pool-" + poolCounter.getAndIncrement();
        threadFactory = new CustomThreadFactory(name, debug);
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
        if (debug) {
            System.out.println("[Pool " + name + "] Shutdown initiated.");
        }
        status = Status.SHUTDOWNED;
    }

    @Override
    public void shutdownNow() {
        if (debug) {
            System.out.println("[Pool " + name + "] Shutdown NOW initiated.");
        }
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
        if (debug) {
            System.out.println("[Pool " + name + "] Worker added: " + worker.thread.getName());
        }
    }

    private void removeWorker(Worker worker) {
        synchronized (workers) {
            workers.remove(worker);
            ensureSpareThreads();
        }
        if (debug) {
            System.out.println("[Pool " + name + "] Worker removed: " + worker.thread.getName());
        }
    }

    private void ensureSpareThreads() {
        if (Status.SHUTDOWNED.equals(status) || Status.SHUTDOWNED_NOW.equals(status)) {
            return;
        }
        synchronized (workers) {
            int spareWorkersCount = 0;
            for (Worker w : workers) {
                if (w.isIdle() && w.getQueueSize() == 0) {
                    spareWorkersCount++;
                }
            }

            int toCreate = Math.min(minSpareThreads - spareWorkersCount, maxPoolSize - workers.size());
            for (int i = 0; i < toCreate; i++) {
                addWorker(new Worker());
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
                if (debug) {
                    System.out.println("[Pool " + name + "] Task accepted into queue of " +
                            worker.thread.getName() + ": " + task);
                }

                ensureSpareThreads();
            } catch (RejectedExecutionException e) {
                task.cancel(false);
                if (debug) {
                    System.out.println("[Rejected " + name + "] Task " + task + " was rejected: " + e.getMessage());
                }

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
        private final BlockingQueue<FutureTask<?>> queue;
        private final Thread thread;
        // Занят ли в текущий момент времени воркер выполнением задачи
        private volatile boolean isIdle;

        public Worker() {
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
                    FutureTask<?> futureTask = queue.poll(keepAliveTime, unit);
                    if (futureTask == null) {
                        if (workers.size() > corePoolSize) {
                            if (debug) {
                                System.out.println("[Worker] " + thread.getName() + " idle timeout, stopping.");
                            }
                            return;
                        }
                        continue;
                    }
                    if (debug) {
                        System.out.println("[Worker] " + thread.getName() + " executes " + futureTask);
                    }

                    isIdle = false;
                    futureTask.run();
                    isIdle = true;
                }
            } catch (InterruptedException e) {
                // ignore
            } finally {
                removeWorker(this);
            }
        }
    }
}

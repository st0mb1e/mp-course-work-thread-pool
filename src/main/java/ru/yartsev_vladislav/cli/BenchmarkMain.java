package ru.yartsev_vladislav.cli;

import ru.yartsev_vladislav.infra.CustomThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BenchmarkMain {
    private static final int TASKS = 200;

    private static final int[] CORES = {1, 2, 4};
    private static final int[] MAXS = {2, 4, 8, 16};
    private static final int[] QUEUES = {10, 50, 100};

    public static void main(String[] args) throws Exception {
        System.out.printf("%-18s %-10s %-10s %-10s %-12s %-10s %-10s\n",
                "config", "custom", "std", "ratio", "cust_rej", "std_rej", "done");

        for (int c : CORES) {
            for (int m : MAXS) {
                if (c > m) {
                    continue;
                }

                for (int q : QUEUES) {

                    Result custom = runCustom(c, m, q);
                    Result std = runStandard(c, m, q);

                    double ratio = (double) custom.time / std.time;

                    String config = "c=" + c + " m=" + m + " q=" + q;

                    System.out.printf("%-18s %-10d %-10d %-10.2f %-10d %-10d %-10d\n",
                            config,
                            custom.time,
                            std.time,
                            ratio,
                            custom.rejected,
                            std.rejected,
                            custom.done
                    );
                }
            }
        }
    }

    private static Result runCustom(int c, int m, int q) throws Exception {
        CustomThreadPool pool = new CustomThreadPool(
                c,
                m,
                5,
                TimeUnit.SECONDS,
                q,
                2,
                false
        );

        List<Future<?>> futures = new ArrayList<>();

        int rejected = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < TASKS; i++) {
            int id = i;

            try {
                futures.add(pool.submit(() -> {
                    simulateWork(id);
                    return null;
                }));
            } catch (RejectedExecutionException e) {
                rejected++;
            }
        }

        int done = waitAll(futures);

        long end = System.currentTimeMillis();

        pool.shutdown();

        return new Result(end - start, rejected, done);
    }

    private static Result runStandard(int c, int m, int q) throws Exception {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                c,
                m,
                5,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(q)
        );

        List<Future<?>> futures = new ArrayList<>();

        int rejected = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < TASKS; i++) {
            int id = i;

            try {
                futures.add(executor.submit(() -> {
                    simulateWork(id);
                    return null;
                }));
            } catch (RejectedExecutionException e) {
                rejected++;
            }
        }

        int done = waitAll(futures);

        long end = System.currentTimeMillis();

        executor.shutdown();

        return new Result(end - start, rejected, done);
    }

    private static int waitAll(List<Future<?>> futures) {
        int done = 0;

        for (Future<?> f : futures) {
            try {
                f.get();
                done++;
            } catch (Exception ignored) {
            }
        }

        return done;
    }

    private static void simulateWork(int id) {
        double x = 0;

        for (int i = 0; i < 10_000; i++) {
            x += Math.sqrt(i) * Math.sin(i);
        }

        try {
            Thread.sleep(5);
        } catch (InterruptedException ignored) {
        }
    }

    private static class Result {
        long time;
        int rejected;
        int done;

        Result(long time, int rejected, int done) {
            this.time = time;
            this.rejected = rejected;
            this.done = done;
        }
    }
}
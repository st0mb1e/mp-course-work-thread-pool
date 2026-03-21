package ru.yartsev_vladislav.cli;

import ru.yartsev_vladislav.infra.CustomThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PoolTuningBenchmarkMain {
    private static final int TASKS = 300;

    public static void main(String[] args) throws Exception {
        System.out.printf("%-20s %-8s %-10s %-10s %-15s %-15s%n",
                "config", "time", "done", "rejected", "throughput", "rejectRate");

        int[] coreSizes = {1, 2, 4, 8};
        int[] maxSizes  = {2, 4, 8, 16};
        int[] queueSizes = {10, 50, 100};
        int[] spare     = {0, 1, 2};

        for (int core : coreSizes) {
            for (int max : maxSizes) {
                if (max < core) continue;

                for (int queue : queueSizes) {
                    for (int sp : spare) {

                        Result r = run(core, max, queue, sp);

                        double throughput = r.done / (r.time / 1000.0);
                        double rejectRate = r.rejected / (double) TASKS;

                        String config = "c=" + core + " m=" + max +
                                " q=" + queue + " s=" + sp;

                        System.out.printf("%-20s %-8d %-10d %-10d %-15.2f %-15.2f%n",
                                config,
                                r.time,
                                r.done,
                                r.rejected,
                                throughput,
                                rejectRate
                        );
                    }
                }
            }
        }
    }

    static class Result {
        long time;
        int done;
        int rejected;

        Result(long time, int done, int rejected) {
            this.time = time;
            this.done = done;
            this.rejected = rejected;
        }
    }

    private static Result run(int core, int max, int queue, int spare) throws Exception {

        CustomThreadPool pool = new CustomThreadPool(
                core,
                max,
                5,
                TimeUnit.SECONDS,
                queue,
                spare,
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

        int done = 0;

        for (Future<?> f : futures) {
            try {
                f.get();
                done++;
            } catch (Exception ignored) {
            }
        }

        long end = System.currentTimeMillis();

        pool.shutdown();

        return new Result(end - start, done, rejected);
    }

    private static void simulateWork(int id) {

        double x = 0;
        for (int i = 0; i < 8_000; i++) {
            x += Math.sin(i) * Math.sqrt(i);
        }

        try {
            Thread.sleep(3);
        } catch (InterruptedException ignored) {}
    }
}
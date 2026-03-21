package ru.yartsev_vladislav.cli;

import ru.yartsev_vladislav.infra.CustomThreadPool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        CustomThreadPool pool = new CustomThreadPool(
                2,
                4,
                5,
                TimeUnit.SECONDS,
                2,
                1
        );

        pool.setRejectedExecutionHandler((task, executor, reason) -> {
            System.out.println("[RejectedHandler] " + reason.getMessage());
        });

        System.out.println("\n=== 1. Загружаем пул выше corePoolSize ===");

        for (int i = 1; i <= 4; i++) {
            int id = i;

            pool.execute(() -> {
                System.out.println("[Task " + id + "] start " + Thread.currentThread().getName());
                sleep(3000);
                System.out.println("[Task " + id + "] end " + Thread.currentThread().getName());
            });
        }

        sleep(1000);

        System.out.println("\n=== 2. Переполняем очередь (должен быть overflow/reject) ===");

        for (int i = 5; i <= 10; i++) {
            int id = i;

            pool.execute(() -> {
                System.out.println("[Overflow Task " + id + "] start");
                sleep(2000);
                System.out.println("[Overflow Task " + id + "] end");
            });
        }

        sleep(2000);

        System.out.println("\n=== 3. Callable + Future ===");

        Future<Integer> future = pool.submit(() -> {
            System.out.println("[Callable] start " + Thread.currentThread().getName());
            sleep(3000);
            System.out.println("[Callable] end");
            return 99;
        });

        System.out.println("[Main] waiting result...");
        System.out.println("[Main] result = " + future.get());

        System.out.println("\n=== 4. minSpareThreads test ===");

        for (int i = 11; i <= 14; i++) {
            int id = i;

            pool.execute(() -> {
                System.out.println("[Spare Task " + id + "] start");
                sleep(4000);
                System.out.println("[Spare Task " + id + "] end");
            });
        }

        sleep(2000);

        System.out.println("\n=== 5. shutdown test ===");
        pool.shutdown();

        try {
            pool.execute(() -> System.out.println("THIS SHOULD FAIL"));
        } catch (RejectedExecutionException e) {
            System.out.println("[Main] rejected after shutdown");
        }

        sleep(6000);

        System.out.println("\n=== 6. shutdownNow test ===");

        CustomThreadPool pool2 = new CustomThreadPool(
                2, 4, 5, TimeUnit.SECONDS, 2, 1
        );

        pool2.execute(() -> {
            System.out.println("[pool2 task] running long task");
            sleep(10000);
        });

        sleep(1000);

        System.out.println("[Main] calling shutdownNow()");
        pool2.shutdownNow();

        sleep(2000);

        System.out.println("\n=== END ===");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
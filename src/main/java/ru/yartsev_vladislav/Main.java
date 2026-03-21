package ru.yartsev_vladislav;

import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // --- Создаём пул ---
        CustomThreadPool pool = new CustomThreadPool(
                2,      // corePoolSize
                4,      // maxPoolSize
                5,      // keepAliveTime
                TimeUnit.SECONDS,
                3,      // queueSize
                1       // minSpareThreads
        );

        // --- Устанавливаем RejectedExecutionHandler ---
        pool.setRejectedExecutionHandler((task, executor, reason) -> {
            System.out.println("[RejectedHandler] Task rejected: " + task + ", reason: " + reason.getMessage());
        });

        // --- Отправляем Runnable задачи ---
        for (int i = 1; i <= 6; i++) {
            int finalI = i;
            pool.execute(() -> {
                System.out.println("[Runnable] Task " + finalI + " started in " + Thread.currentThread().getName());
                try {
                    Thread.sleep(2000); // имитация работы
                } catch (InterruptedException e) {
                    System.out.println("[Runnable] Task " + finalI + " was interrupted");
                }
                System.out.println("[Runnable] Task " + finalI + " finished in " + Thread.currentThread().getName());
            });
        }

        // --- Отправляем Callable задачи ---
        Future<Integer> futureResult = pool.submit(() -> {
            System.out.println("[Callable] Task started in " + Thread.currentThread().getName());
            Thread.sleep(3000);
            System.out.println("[Callable] Task finished in " + Thread.currentThread().getName());
            return 42;
        });

        // --- Проверяем результат Callable ---
        try {
            Integer result = futureResult.get(); // блокируется до завершения
            System.out.println("[Main] Callable returned: " + result);
        } catch (CancellationException e) {
            System.out.println("[Main] Callable was cancelled");
        }

        // --- Демонстрируем shutdown ---
        System.out.println("[Main] Shutting down pool...");
        pool.shutdown();

        // Попытка добавить новую задачу после shutdown
        try {
            pool.execute(() -> System.out.println("This task should be rejected"));
        } catch (RejectedExecutionException e) {
            System.out.println("[Main] Task rejected after shutdown: " + e.getMessage());
        }

        // --- Ждём завершения всех потоков ---
        Thread.sleep(7000); // ждём завершения текущих задач
        System.out.println("[Main] All done");
    }
}
package ru.yartsev_vladislav.port;

import ru.yartsev_vladislav.infra.CustomThreadPool;

import java.util.concurrent.RejectedExecutionException;

@FunctionalInterface
public interface CustomRejectedExecutionHandler {
    void rejectedExecution(Runnable task, CustomThreadPool pool, RejectedExecutionException exception);
}

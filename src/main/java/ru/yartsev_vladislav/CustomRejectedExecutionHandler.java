package ru.yartsev_vladislav;

import java.util.concurrent.RejectedExecutionException;

@FunctionalInterface
public interface CustomRejectedExecutionHandler {
    void rejectedExecution(Runnable task, CustomThreadPool pool, RejectedExecutionException exception);
}

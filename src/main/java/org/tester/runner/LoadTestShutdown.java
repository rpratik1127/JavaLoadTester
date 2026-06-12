package org.tester.runner;

import org.tester.config.TestConstants;
import org.tester.executor.HttpExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates graceful shutdown of virtual-user executors and in-flight HTTP work.
 */
public final class LoadTestShutdown {

    private LoadTestShutdown() {
    }

    public static void drainExecutor(ExecutorService executorService, long timeoutSeconds)
            throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
    }

    /** Waits for Netty in-flight HTTP to finish so final metrics are stable. */
    public static void drainInflightHttp() throws InterruptedException {
        HttpExecutor.waitForInflightDrain(TestConstants.httpDrainTimeoutMs());
    }

    public static void drainExecutorAndInflightHttp(ExecutorService executorService, long timeoutSeconds)
            throws InterruptedException {
        drainExecutor(executorService, timeoutSeconds);
        drainInflightHttp();
    }
}

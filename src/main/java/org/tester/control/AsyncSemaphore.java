package org.tester.control;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Non-blocking semaphore. Callers chain on {@link #acquire()} instead of parking threads.
 */
public class AsyncSemaphore {

    private final int maxPermits;
    private final AtomicInteger inUse = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<CompletableFuture<Void>> waiters =
            new ConcurrentLinkedQueue<>();

    public AsyncSemaphore(int maxPermits) {
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        this.maxPermits = maxPermits;
    }

    public CompletableFuture<Void> acquire() {
        while (true) {
            int current = inUse.get();
            if (current >= maxPermits) {
                break;
            }
            if (inUse.compareAndSet(current, current + 1)) {
                return CompletableFuture.completedFuture(null);
            }
        }

        CompletableFuture<Void> waiter = new CompletableFuture<>();
        waiters.add(waiter);
        return waiter;
    }

    public void release() {
        // Prefer handing the permit to a queued acquirer over decrementing inUse.
        CompletableFuture<Void> waiter = waiters.poll();
        if (waiter != null) {
            waiter.complete(null);
            return;
        }

        int current;
        do {
            current = inUse.get();
            if (current <= 0) {
                return;
            }
        } while (!inUse.compareAndSet(current, current - 1));
    }

    public int availablePermits() {
        return Math.max(0, maxPermits - inUse.get());
    }
}

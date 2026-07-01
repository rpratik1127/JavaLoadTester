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

    /** Creates a non-blocking concurrency limiter with the given permit count. */
    public AsyncSemaphore(int maxPermits) {
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be positive");
        }
        this.maxPermits = maxPermits;
    }

    public CompletableFuture<Void> acquire() {
        if (tryAcquirePermit()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> waiter = new CompletableFuture<>();
        waiters.add(waiter);

        // Lost-wakeup guard: a release may have happened between the failed try and enqueue.
        if (tryAcquirePermit()) {
            waiters.remove(waiter);
            if (!waiter.isDone()) {
                waiter.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }

        return waiter;
    }

    private boolean tryAcquirePermit() {
        while (true) {
            int current = inUse.get();
            if (current >= maxPermits) {
                return false;
            }
            if (inUse.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /** Releases a permit and hands it to the next waiter or decrements in-use count. */
    public void release() {
        CompletableFuture<Void> waiter = waiters.poll();
        if (waiter != null) {
            if (!waiter.isDone()) {
                waiter.complete(null);
            }
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

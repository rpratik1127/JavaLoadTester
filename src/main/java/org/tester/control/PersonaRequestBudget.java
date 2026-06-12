package org.tester.control;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single source of truth for total-request mode.
 * Atomically enforces both the hard request cap and the time-based release schedule.
 */
public class PersonaRequestBudget {

    public static final int TICK_MS = 10;

    private final int limit;
    private final long startMillis;
    private final long durationMillis;

    private final AtomicInteger consumed = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<CompletableFuture<Boolean>> waiters =
            new ConcurrentLinkedQueue<>();

    public PersonaRequestBudget(int limit, long startMillis, long durationMillis) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (durationMillis <= 0) {
            throw new IllegalArgumentException("durationMillis must be positive");
        }

        this.limit = limit;
        this.startMillis = startMillis;
        this.durationMillis = durationMillis;

        BudgetScheduler.register(this);
    }

    /** Called every {@link #TICK_MS} by {@link BudgetScheduler} to wake queued acquirers. */
    void tick() {
        drainWaiters();
    }

    public CompletableFuture<Boolean> acquire() {
        if (tryConsume()) {
            return CompletableFuture.completedFuture(true);
        }

        if (isExhausted() || isExpired()) {
            return CompletableFuture.completedFuture(false);
        }

        // No permit yet — queue until the next tick releases more or budget is exhausted.
        CompletableFuture<Boolean> waiter = new CompletableFuture<>();
        waiters.add(waiter);
        waiter.whenComplete((ignored, error) -> waiters.remove(waiter));
        drainWaiters();
        return waiter;
    }

    private boolean tryConsume() {
        if (isExpired()) {
            return false;
        }

        long permitted = getPermittedByNow();
        while (true) {
            int current = consumed.get();
            if (current >= limit || current >= permitted) {
                return false;
            }
            if (consumed.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Linearly releases permits over the test window.
     * Example: 1000 requests / 100s → 100 permits available after 10s.
     */
    public long getPermittedByNow() {
        long elapsed = System.currentTimeMillis() - startMillis;
        if (elapsed <= 0) {
            return 0;
        }
        if (elapsed >= durationMillis) {
            return limit;
        }
        // Ceiling division so pacing does not systematically under-release permits.
        return Math.min(limit, (limit * elapsed + durationMillis - 1) / durationMillis);
    }

    public boolean isExhausted() {
        return consumed.get() >= limit;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - startMillis >= durationMillis;
    }

    /** Wakes waiters when permits are exhausted or the test window has ended. */
    private void drainWaiters() {
        if (isExhausted() || isExpired()) {
            CompletableFuture<Boolean> waiter;
            while ((waiter = waiters.poll()) != null) {
                if (!waiter.isDone()) {
                    waiter.complete(false);
                }
            }
            return;
        }

        while (true) {
            CompletableFuture<Boolean> waiter = waiters.peek();
            if (waiter == null) {
                return;
            }

            if (waiter.isDone()) {
                waiters.poll();
                continue;
            }

            if (!tryConsume()) {
                return;
            }

            waiter = waiters.poll();
            if (waiter != null && !waiter.isDone()) {
                waiter.complete(true);
            }
        }
    }

    public int getLimit() {
        return limit;
    }

    public int getConsumed() {
        return consumed.get();
    }

    /**
     * Returns a permit to the budget when it was acquired but the HTTP send was skipped.
     */
    public boolean releaseOne() {
        while (true) {
            int current = consumed.get();
            if (current <= 0) {
                return false;
            }
            if (consumed.compareAndSet(current, current - 1)) {
                drainWaiters();
                return true;
            }
        }
    }

    public void shutdown() {
        waiters.forEach(waiter -> {
            if (!waiter.isDone()) {
                waiter.complete(false);
            }
        });
        waiters.clear();
    }
}

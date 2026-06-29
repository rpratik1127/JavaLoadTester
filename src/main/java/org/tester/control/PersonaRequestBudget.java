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
    /** Extra permits beyond the linear schedule so VUs can send without waiting at t=0. */
    private static final int PERMIT_LOOKAHEAD_SEC = 5;

    private final int limit;
    private final int targetRps;
    private final long startMillis;
    private final long durationMillis;

    private final AtomicInteger consumed = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<CompletableFuture<Boolean>> waiters =
            new ConcurrentLinkedQueue<>();

    public PersonaRequestBudget(int limit, long startMillis, long durationMillis, int targetRps) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (durationMillis <= 0) {
            throw new IllegalArgumentException("durationMillis must be positive");
        }
        if (targetRps <= 0) {
            throw new IllegalArgumentException("targetRps must be positive");
        }

        this.limit = limit;
        this.targetRps = targetRps;
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

        if (isExhausted()) {
            return CompletableFuture.completedFuture(false);
        }

        if (isExpired()) {
            return tryConsume()
                    ? CompletableFuture.completedFuture(true)
                    : CompletableFuture.completedFuture(false);
        }

        CompletableFuture<Boolean> waiter = new CompletableFuture<>();
        waiters.add(waiter);
        waiter.whenComplete((ignored, error) -> waiters.remove(waiter));

        // Lost-wakeup guard: a tick may have released permits between try and enqueue.
        if (tryConsume()) {
            waiters.remove(waiter);
            if (!waiter.isDone()) {
                waiter.complete(true);
            }
            return CompletableFuture.completedFuture(true);
        }

        drainWaiters();
        return waiter;
    }

    /** Non-blocking fast path — avoids waiter queue when a permit is already available. */
    public boolean tryAcquireNow() {
        return tryConsume();
    }

    private boolean tryConsume() {
        if (consumed.get() >= limit) {
            return false;
        }

        // After the rate window, drain any permits the schedule could not deliver in time.
        if (isExpired()) {
            while (true) {
                int current = consumed.get();
                if (current >= limit) {
                    return false;
                }
                if (consumed.compareAndSet(current, current + 1)) {
                    return true;
                }
            }
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
        if (elapsed >= durationMillis) {
            return limit;
        }

        long linear = elapsed <= 0
                ? 0
                : Math.min(limit, (limit * elapsed + durationMillis - 1) / durationMillis);
        long lookahead = (long) targetRps * PERMIT_LOOKAHEAD_SEC;
        long permitted = Math.min(limit, linear + lookahead);

        // Final quarter: if sends are behind the linear schedule, release all remaining permits.
        if (elapsed >= durationMillis * 3 / 4) {
            long linearGoal = linear;
            if (consumed.get() < linearGoal * PROGRESS_THRESHOLD) {
                return limit;
            }
        }

        return permitted;
    }

    private static final double PROGRESS_THRESHOLD = 0.95;

    public boolean isExhausted() {
        return consumed.get() >= limit;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - startMillis >= durationMillis;
    }

    /** Wakes waiters when permits are exhausted; after expiry, drains the remaining budget. */
    private void drainWaiters() {
        if (isExhausted()) {
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

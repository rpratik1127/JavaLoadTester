package org.tester.control;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async token-bucket scheduler. Controls request rate without blocking threads.
 */
public class RpsScheduler {

    private static final int REFILL_INTERVAL_MS = 10;

    private final int targetRps;
    private final int maxTokens;
    private final AtomicInteger availableTokens = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<CompletableFuture<Void>> waiters =
            new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<CompletableFuture<Void>, Boolean> registeredWaiters =
            new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicLong acquiredTotal = new AtomicLong(0);
    /** Fractional tokens below 1 are carried forward between refill ticks. */
    private double storedFraction = 0.0;

    public RpsScheduler(int targetRps) {
        this(targetRps, targetRps);
    }

    public RpsScheduler(int targetRps, int maxBurstTokens) {
        if (targetRps <= 0) {
            throw new IllegalArgumentException("targetRps must be positive");
        }
        if (maxBurstTokens < targetRps) {
            throw new IllegalArgumentException("maxBurstTokens must be >= targetRps");
        }

        this.targetRps = targetRps;
        this.maxTokens = maxBurstTokens;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rps-scheduler");
            t.setDaemon(true);
            return t;
        });

        startRefill();
    }

    private void startRefill() {
        double tokensPerInterval = targetRps * (REFILL_INTERVAL_MS / 1000.0);

        scheduler.scheduleAtFixedRate(() -> {
            double total = tokensPerInterval + storedFraction;
            int toAdd = (int) total;
            storedFraction = total - toAdd;

            if (toAdd <= 0) {
                return;
            }

            int current = availableTokens.get();
            int canAdd = Math.max(0, maxTokens - current);
            int actual = Math.min(toAdd, canAdd);

            if (actual > 0) {
                availableTokens.addAndGet(actual);
            }

            drainWaiters();
        }, 0, REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> acquire() {
        if (tryTakeToken()) {
            acquiredTotal.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> waiter = new CompletableFuture<>();
        registeredWaiters.put(waiter, Boolean.TRUE);
        waiters.add(waiter);

        waiter.whenComplete((ignored, error) -> registeredWaiters.remove(waiter));

        drainWaiters();
        return waiter;
    }

    public boolean tryAcquire() {
        if (!tryTakeToken()) {
            return false;
        }
        acquiredTotal.incrementAndGet();
        return true;
    }

    public void cancel(CompletableFuture<Void> waiter) {
        if (waiter == null) {
            return;
        }
        registeredWaiters.remove(waiter);
        waiter.completeExceptionally(new java.util.concurrent.CancellationException("RPS acquire cancelled"));
    }

    private boolean tryTakeToken() {
        while (true) {
            int current = availableTokens.get();
            if (current <= 0) {
                return false;
            }
            if (availableTokens.compareAndSet(current, current - 1)) {
                return true;
            }
        }
    }

    private void drainWaiters() {
        while (true) {
            int current = availableTokens.get();
            if (current <= 0) {
                return;
            }

            CompletableFuture<Void> waiter = waiters.poll();
            if (waiter == null) {
                return;
            }

            if (!registeredWaiters.containsKey(waiter) || waiter.isDone()) {
                continue;
            }

            if (availableTokens.compareAndSet(current, current - 1)) {
                acquiredTotal.incrementAndGet();
                waiter.complete(null);
            } else {
                waiters.add(waiter);
                return;
            }
        }
    }

    public int getTargetRps() {
        return targetRps;
    }

    public int availableTokens() {
        return availableTokens.get();
    }

    public long getAcquiredTotal() {
        return acquiredTotal.get();
    }

    public void shutdown() {
        scheduler.shutdownNow();
        waiters.forEach(waiter ->
                waiter.completeExceptionally(new IllegalStateException("RPS scheduler shut down"))
        );
        waiters.clear();
        registeredWaiters.clear();
    }
}

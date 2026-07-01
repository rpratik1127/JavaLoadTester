package org.tester.control;

import java.util.concurrent.CompletableFuture;

/**
 * Async throughput gate backed by a token-bucket {@link RpsScheduler}.
 */
public class ThroughputController {

    private final RpsScheduler rpsScheduler;
    private final int targetTps;

    /** Creates a global async TPS gate; target must be positive. */
    public ThroughputController(int targetTps) {
        if (targetTps <= 0) {
            throw new IllegalArgumentException("Target TPS must be greater than 0");
        }

        this.targetTps = targetTps;
        this.rpsScheduler = new RpsScheduler(targetTps);

        System.out.printf(
                "[ThroughputController] async token-bucket enabled, targetTps=%d%n",
                targetTps
        );
    }

    /** Waits asynchronously for a send permit from the token bucket. */
    public CompletableFuture<Void> acquireAsync() {
        return rpsScheduler.acquire();
    }

    /** Non-blocking attempt to take a send permit; returns false when rate-limited. */
    public boolean tryAcquire() {
        return rpsScheduler.tryAcquire();
    }

    /** Cancels a pending async acquire and fails the associated future. */
    public void cancelAcquire(CompletableFuture<Void> permitFuture) {
        rpsScheduler.cancel(permitFuture);
    }

    /** Stops the underlying scheduler and fails any queued waiters. */
    public void shutdown() {
        rpsScheduler.shutdown();
    }

    public int getTargetTps() {
        return targetTps;
    }

    public int availablePermits() {
        return rpsScheduler.availableTokens();
    }
}

package org.tester.control;

import java.util.concurrent.CompletableFuture;

/**
 * Async throughput gate backed by a token-bucket {@link RpsScheduler}.
 */
public class ThroughputController {

    private final RpsScheduler rpsScheduler;
    private final int targetTps;

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

    public CompletableFuture<Void> acquireAsync() {
        return rpsScheduler.acquire();
    }

    public boolean tryAcquire() {
        return rpsScheduler.tryAcquire();
    }

    public void cancelAcquire(CompletableFuture<Void> permitFuture) {
        rpsScheduler.cancel(permitFuture);
    }

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

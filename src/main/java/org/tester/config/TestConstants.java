package org.tester.config;

/**
 * Shared constants for load-test orchestration and reporting.
 */
public final class TestConstants {

    public static final String DEFAULT_PERSONA_FILE = "personas/persona.json";
    public static final String STEP_REPORT_FILE = "step-report.csv";
    public static final String REQUEST_LOG_FILE = "request-log.csv";

    public static final long LIVE_REPORT_INTERVAL_MS = 2_000;
    public static final long REQUEST_MODE_POLL_MS = 100;
    public static final long REQUEST_MODE_END_DRAIN_MS = 1_000;
    /** Max wait after the rate window for unfired budget permits to drain. */
    public static final long REQUEST_MODE_BUDGET_DRAIN_MS = 120_000L;

    /** Max wait for in-flight HTTP after the test window ends (not the per-request timeout). */
    public static final long HTTP_DRAIN_TIMEOUT_MS = 10_000L;
    /** Max wait for virtual-user executor threads after shutdown. */
    public static final long EXECUTOR_DRAIN_SEC = 10L;

    /**
     * Target steady-state throughput. Used for pool warmup and VU cap sizing
     * (Little's law: connections ≈ TPS × RTT).
     */
    public static final int TARGET_TPS = Integer.getInteger("tester.target.tps", 1_600);

    /**
     * Persistent TCP connections per host. For 1600 TPS at ~250 ms server RTT,
     * provision at least 400 parallel connections.
     */
    public static final int MAX_POOL_CONNECTIONS =
            Integer.getInteger("tester.pool.connections", 500);

    /**
     * In-flight HTTP requests per TCP connection. POST endpoints typically serialize
     * per connection; depth 1 avoids client-side queue inflation.
     */
    public static final int MAX_PIPELINE_PER_CHANNEL =
            Integer.getInteger("tester.pipeline.perChannel", 4);

    /**
     * Logical request pumps per virtual user. One pump per user matches one
     * dedicated keep-alive connection (Fusillade-style closed loop).
     */
    public static final int PIPELINE_DEPTH_PER_USER = Math.max(
            1,
            Integer.getInteger("tester.pipeline.depth", 4)
    );

    /** Upper bound on pre-opened pooled connections per host. */
    public static final int POOL_WARMUP_MAX = 2_000;
    /** Floor on pool warmup — sized for {@link #TARGET_TPS} at moderate RTT. */
    public static final int POOL_WARMUP_MIN = Integer.getInteger(
            "tester.pool.warmup.min",
            Math.max(500, TARGET_TPS / 4)
    );

    private TestConstants() {
    }

    /** Exposes the HTTP in-flight drain timeout for shutdown coordination. */
    public static long httpDrainTimeoutMs() {
        return HTTP_DRAIN_TIMEOUT_MS;
    }

    /** Exposes the virtual-thread executor drain timeout for shutdown coordination. */
    public static long executorDrainSeconds() {
        return EXECUTOR_DRAIN_SEC;
    }

    /**
     * Estimates virtual users needed for a target RPS at the given average latency (seconds).
     */
    public static int estimateUsersForRps(int targetRps, double avgLatencySec) {
        if (avgLatencySec <= 0) {
            avgLatencySec = 0.25;
        }
        return Math.max(1, (int) Math.ceil(targetRps * avgLatencySec * 1.25));
    }
}

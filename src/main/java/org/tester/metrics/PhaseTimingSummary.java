package org.tester.metrics;

/**
 * Average phase durations (ms) aggregated across completed requests.
 */
public record PhaseTimingSummary(
        long sampleCount,
        double toAcquireStartMs,
        double acquireMs,
        double queueToWriteMs,
        double writeMs,
        double serverTtfbMs,
        double headerToFirstChunkMs,
        double bodyOnWireMs,
        double aggregateWaitMs,
        double bodyMs,
        double totalMs
) {
    /** Sentinel summary when no requests recorded phase timings. */
    public static PhaseTimingSummary empty() {
        return new PhaseTimingSummary(0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1);
    }
}

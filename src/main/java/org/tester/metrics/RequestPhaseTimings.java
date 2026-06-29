package org.tester.metrics;

/**
 * Nanosecond timestamps for major HTTP client phases on a single request.
 * Durations are derived as deltas between consecutive phases.
 */
public class RequestPhaseTimings {

    /** {@link org.tester.executor.HttpExecutor} accepted the send (beginSend). */
    public long requestCreatedNanos;
    /** Pool / dedicated-channel lookup or acquire started. */
    public long connectionAcquireStartNanos;
    /** Channel is bound and ready for send. */
    public long connectionAcquiredNanos;
    /** Immediately before {@code writeAndFlush} on the event loop. */
    public long beforeWriteNanos;
    /** Netty write future completed successfully. */
    public long writeCompletedNanos;
    /** First HTTP response headers decoded on the event loop (TTFB). */
    public long firstResponseNanos;
    /**
     * First response body chunk ({@code HttpContent}) decoded on the event loop.
     * Empty bodies may only produce a {@code LastHttpContent} chunk.
     */
    public long firstBodyChunkNanos;
    /**
     * Last inbound HTTP object (headers or body chunk) processed on the event loop
     * before the response aggregator emits the full message.
     */
    public long lastInboundReadNanos;
    /** Full response body aggregated and application handler invoked. */
    public long fullResponseNanos;

    public long toAcquireStartMs() {
        return deltaMs(requestCreatedNanos, connectionAcquireStartNanos);
    }

    public long acquireMs() {
        return deltaMs(connectionAcquireStartNanos, connectionAcquiredNanos);
    }

    /** In-flight permit + event-loop scheduling wait before bytes are written. */
    public long queueToWriteMs() {
        return deltaMs(connectionAcquiredNanos, beforeWriteNanos);
    }

    public long writeMs() {
        return deltaMs(beforeWriteNanos, writeCompletedNanos);
    }

    /** Time from write complete to first response headers (server + network TTFB). */
    public long serverTtfbMs() {
        return deltaMs(writeCompletedNanos, firstResponseNanos);
    }

    /** Time from response headers to first body chunk seen on the event loop. */
    public long headerToFirstChunkMs() {
        return deltaMs(firstResponseNanos, firstBodyChunkNanos);
    }

    /**
     * Time from first body chunk to last inbound chunk processed on the event loop.
     * Approximates body transfer as seen by the client I/O thread (wire + read scheduling).
     */
    public long bodyOnWireMs() {
        long bodyStart = firstBodyChunkNanos > 0 ? firstBodyChunkNanos : firstResponseNanos;
        return deltaMs(bodyStart, lastInboundReadNanos);
    }

    /**
     * Time from last inbound chunk on the event loop until the aggregated response is delivered.
     * Large values indicate event-loop backlog after all bytes were already decoded.
     */
    public long aggregateWaitMs() {
        if (lastInboundReadNanos <= 0) {
            return deltaMs(firstResponseNanos, fullResponseNanos);
        }
        return deltaMs(lastInboundReadNanos, fullResponseNanos);
    }

    /** Time from first headers to full body available (sum of body sub-phases). */
    public long bodyMs() {
        return deltaMs(firstResponseNanos, fullResponseNanos);
    }

    public long totalMs() {
        return deltaMs(requestCreatedNanos, fullResponseNanos);
    }

    private static long deltaMs(long startNanos, long endNanos) {
        if (startNanos <= 0 || endNanos <= 0 || endNanos < startNanos) {
            return -1;
        }
        return (endNanos - startNanos) / 1_000_000L;
    }
}

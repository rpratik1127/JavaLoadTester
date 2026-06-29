package org.tester.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide HTTP connection and pipelining counters for throughput diagnostics.
 */
public final class HttpConnectionMetrics {

    private static final AtomicLong activeChannels = new AtomicLong();
    private static final AtomicLong channelsCreated = new AtomicLong();
    private static final AtomicLong requestsOnConnections = new AtomicLong();

    private HttpConnectionMetrics() {
    }

    public static void channelActivated() {
        activeChannels.incrementAndGet();
        channelsCreated.incrementAndGet();
    }

    public static void channelDeactivated() {
        activeChannels.decrementAndGet();
    }

    public static void recordRequestOnConnection() {
        requestsOnConnections.incrementAndGet();
    }

    public static long getActiveChannels() {
        return activeChannels.get();
    }

    public static long getChannelsCreated() {
        return channelsCreated.get();
    }

    public static long getRequestsOnConnections() {
        return requestsOnConnections.get();
    }

    /** Average completed requests per channel since process start. */
    public static double getRequestsPerConnection() {
        long channels = channelsCreated.get();
        return channels == 0 ? 0 : (double) requestsOnConnections.get() / channels;
    }

    /** Fraction of requests that reused an existing channel (1 - 1/reqPerConn). */
    public static double getConnectionReuseRate() {
        double perConn = getRequestsPerConnection();
        if (perConn <= 1.0) {
            return 0;
        }
        return (perConn - 1.0) / perConn;
    }
}

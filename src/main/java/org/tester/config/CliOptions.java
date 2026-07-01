package org.tester.config;

import org.tester.executor.ConnectionMode;

/**
 * Command-line flags for {@link org.tester.Main}.
 */
public final class CliOptions {

    /** When false, skip writing {@code request-log.csv} for faster runs. */
    public final boolean generateRequestLog;
    /** When true, record send-time RPS separately from completed-response throughput. */
    public final boolean trackSentRps;
    /** Pooled (default) or sticky per-user HTTP connections. */
    public final ConnectionMode connectionMode;

    /** Immutable snapshot of parsed CLI flags. */
    private CliOptions(boolean generateRequestLog, boolean trackSentRps, ConnectionMode connectionMode) {
        this.generateRequestLog = generateRequestLog;
        this.trackSentRps = trackSentRps;
        this.connectionMode = connectionMode;
    }

    /** Parses supported flags from {@code args}; unrecognized tokens are ignored. */
    public static CliOptions fromArgs(String[] args) {
        boolean generateRequestLog = false;
        boolean trackSentRps = false;
        ConnectionMode connectionMode = ConnectionMode.POOLED;

        for (String arg : args) {
            if ("--no-request-log".equals(arg)) {
                generateRequestLog = false;
            } else if ("--request-log".equals(arg)) {
                generateRequestLog = true;
            } else if ("--track-sent-rps".equals(arg)) {
                trackSentRps = true;
            } else if ("--sticky-connections".equals(arg)) {
                connectionMode = ConnectionMode.STICKY;
            }
        }

        return new CliOptions(generateRequestLog, trackSentRps, connectionMode);
    }
}

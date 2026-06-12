package org.tester.config;

import org.tester.executor.HttpExecutor;

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

    /** Grace period beyond {@link HttpExecutor#getRequestTimeoutMillis()} for executor/HTTP drain. */
    public static final long HTTP_DRAIN_BUFFER_MS = 5_000L;

    private TestConstants() {
    }

    public static long httpDrainTimeoutMs() {
        return HttpExecutor.getRequestTimeoutMillis() + HTTP_DRAIN_BUFFER_MS;
    }

    public static long executorDrainSeconds() {
        return HttpExecutor.getRequestTimeoutMillis() / 1000L + HTTP_DRAIN_BUFFER_MS / 1000L;
    }
}

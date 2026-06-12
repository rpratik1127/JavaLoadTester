package org.tester.metrics;

/** Why a request was marked unsuccessful in {@link RequestMetric}. */
public enum RequestFailureReason {
    NONE,
    TIMEOUT,
    CHANNEL_CLOSED,
    CHANNEL_INACTIVE,
    WRITE_FAILED,
    EXTRACTION_FAILED,
    HTTP_STATUS_MISMATCH,
    UNSUPPORTED_SCHEME,
    POOL_ACQUIRE_FAILED,
    CANCELLED,
    EXECUTION_ERROR,
    UNKNOWN
}

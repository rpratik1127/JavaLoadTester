package org.tester.metrics;

/** Immutable result of one completed (or failed) step execution. */
public class RequestMetric {

    /** Virtual user that executed the step. */
    public String userId;
    public String personaName;
    public String stepName;
    /** HTTP status code; 0 when no response was received. */
    public int statusCode;
    public long responseTimeMs;
    public boolean success;
    /** Wall-clock time when the metric was recorded. */
    public long timestamp;
    public RequestFailureReason failureReason;
    public String errorDetail;

    public RequestMetric(
            String userId,
            String personaName,
            String stepName,
            int statusCode,
            long responseTimeMs,
            boolean success
    ) {
        this(userId, personaName, stepName, statusCode, responseTimeMs, success,
                success ? RequestFailureReason.NONE : RequestFailureReason.UNKNOWN, null);
    }

    public RequestMetric(
            String userId,
            String personaName,
            String stepName,
            int statusCode,
            long responseTimeMs,
            boolean success,
            RequestFailureReason failureReason,
            String errorDetail
    ) {
        this.userId = userId;
        this.personaName = personaName;
        this.stepName = stepName;
        this.statusCode = statusCode;
        this.responseTimeMs = responseTimeMs;
        this.success = success;
        this.failureReason = failureReason == null ? RequestFailureReason.NONE : failureReason;
        this.errorDetail = errorDetail;
        this.timestamp = System.currentTimeMillis();
    }

    public static RequestMetric failed(
            String userId,
            String personaName,
            String stepName,
            RequestFailureReason reason,
            String errorDetail
    ) {
        return new RequestMetric(
                userId,
                personaName,
                stepName,
                0,
                0,
                false,
                reason,
                errorDetail
        );
    }

    public static RequestMetric failed(String userId, String personaName, String stepName, String errorMessage) {
        return failed(userId, personaName, stepName, RequestFailureReason.EXECUTION_ERROR, errorMessage);
    }
}

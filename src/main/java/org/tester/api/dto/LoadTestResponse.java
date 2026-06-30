package org.tester.api.dto;

import java.util.Map;

public class LoadTestResponse {

    private String executionId;
    private LoadTestStatus status;
    private long totalRequests;
    private long successfulRequests;
    private long failedRequests;
    private double errorRate;
    private double throughput;
    private double averageLatencyMs;
    private int executionDurationSeconds;
    private String stepReportPath;
    private String requestLogPath;
    private long minLatencyMs;
    private long maxLatencyMs;
    private long p50LatencyMs;
    private long p90LatencyMs;
    private long p95LatencyMs;
    private long p99LatencyMs;
    private long p999LatencyMs;
    private double maxRequestsSentPerSecond;
    private double averageRequestsSentPerSecond;
    private Map<Integer, Long> failedStatusCodeCounts;
    private String errorMessage;

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public LoadTestStatus getStatus() {
        return status;
    }

    public void setStatus(LoadTestStatus status) {
        this.status = status;
    }

    public long getTotalRequests() {
        return totalRequests;
    }

    public void setTotalRequests(long totalRequests) {
        this.totalRequests = totalRequests;
    }

    public long getSuccessfulRequests() {
        return successfulRequests;
    }

    public void setSuccessfulRequests(long successfulRequests) {
        this.successfulRequests = successfulRequests;
    }

    public long getFailedRequests() {
        return failedRequests;
    }

    public void setFailedRequests(long failedRequests) {
        this.failedRequests = failedRequests;
    }

    public double getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }

    public double getThroughput() {
        return throughput;
    }

    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    public double getAverageLatencyMs() {
        return averageLatencyMs;
    }

    public void setAverageLatencyMs(double averageLatencyMs) {
        this.averageLatencyMs = averageLatencyMs;
    }

    public int getExecutionDurationSeconds() {
        return executionDurationSeconds;
    }

    public void setExecutionDurationSeconds(int executionDurationSeconds) {
        this.executionDurationSeconds = executionDurationSeconds;
    }

    public String getStepReportPath() {
        return stepReportPath;
    }

    public void setStepReportPath(String stepReportPath) {
        this.stepReportPath = stepReportPath;
    }

    public String getRequestLogPath() {
        return requestLogPath;
    }

    public void setRequestLogPath(String requestLogPath) {
        this.requestLogPath = requestLogPath;
    }

    public long getMinLatencyMs() {
        return minLatencyMs;
    }

    public void setMinLatencyMs(long minLatencyMs) {
        this.minLatencyMs = minLatencyMs;
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setMaxLatencyMs(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    public long getP50LatencyMs() {
        return p50LatencyMs;
    }

    public void setP50LatencyMs(long p50LatencyMs) {
        this.p50LatencyMs = p50LatencyMs;
    }

    public long getP90LatencyMs() {
        return p90LatencyMs;
    }

    public void setP90LatencyMs(long p90LatencyMs) {
        this.p90LatencyMs = p90LatencyMs;
    }

    public long getP95LatencyMs() {
        return p95LatencyMs;
    }

    public void setP95LatencyMs(long p95LatencyMs) {
        this.p95LatencyMs = p95LatencyMs;
    }

    public long getP99LatencyMs() {
        return p99LatencyMs;
    }

    public void setP99LatencyMs(long p99LatencyMs) {
        this.p99LatencyMs = p99LatencyMs;
    }

    public long getP999LatencyMs() {
        return p999LatencyMs;
    }

    public void setP999LatencyMs(long p999LatencyMs) {
        this.p999LatencyMs = p999LatencyMs;
    }

    public double getMaxRequestsSentPerSecond() {
        return maxRequestsSentPerSecond;
    }

    public void setMaxRequestsSentPerSecond(double maxRequestsSentPerSecond) {
        this.maxRequestsSentPerSecond = maxRequestsSentPerSecond;
    }

    public double getAverageRequestsSentPerSecond() {
        return averageRequestsSentPerSecond;
    }

    public void setAverageRequestsSentPerSecond(double averageRequestsSentPerSecond) {
        this.averageRequestsSentPerSecond = averageRequestsSentPerSecond;
    }

    public Map<Integer, Long> getFailedStatusCodeCounts() {
        return failedStatusCodeCounts;
    }

    public void setFailedStatusCodeCounts(Map<Integer, Long> failedStatusCodeCounts) {
        this.failedStatusCodeCounts = failedStatusCodeCounts;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}

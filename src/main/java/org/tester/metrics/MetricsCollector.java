package org.tester.metrics;

import org.tester.model.ApiStep;
import org.tester.model.Persona;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe, scalable metrics collector for HTTP load testing.
 * <p>
 * Hot path is {@link #add(RequestMetric)}; persona/step counters are pre-registered
 * via {@link #registerPersonas(java.util.List)} to avoid map contention at scale.
 */
public class MetricsCollector {

    // -------------------------------------------------------------------------
    // Global counters
    // -------------------------------------------------------------------------

    private final LongAdder totalRequests   = new LongAdder();
    private final LongAdder successCount    = new LongAdder();
    private final LongAdder failureCount    = new LongAdder();
    private final LongAdder totalResponseMs = new LongAdder();
    private final AtomicLong minResponseMs  = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxResponseMs  = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Requests Sent Per Second by users
    // -------------------------------------------------------------------------

    private final LongAdder totalRequestsSent = new LongAdder();

    /**
     * epochSecond -> number of requests sent in that second.
     * Used for max and average requests sent/sec.
     */
    private final ConcurrentHashMap<Long, LongAdder> requestsSentPerSecond =
            new ConcurrentHashMap<>();

    private static final int SENT_RPS_WINDOW_SIZE = 100_000;

    private final AtomicLongArray sentRequestTimeRing =
            new AtomicLongArray(SENT_RPS_WINDOW_SIZE);

    private final AtomicLong sentRequestIndex =
            new AtomicLong(0);

    private final LongAdder liveSuccessCounter = new LongAdder();

    private static final long HISTO_SAMPLE_MIN_REQUESTS = 10_000;

    private volatile boolean recordDetailedRequests = true;
    private volatile boolean trackSentRps = false;

    // -------------------------------------------------------------------------
    // Failed status-code counters
    // -------------------------------------------------------------------------

    private final ConcurrentHashMap<Integer, LongAdder> failedStatusCodeCounts =
            new ConcurrentHashMap<>();

    private static final Set<Integer> KNOWN_FAILED_STATUS_CODES =
            Set.of(0, 400, 401, 403, 404, 429, 500, 502, 503, 504);

    // -------------------------------------------------------------------------
    // Per-persona / per-step counters
    // -------------------------------------------------------------------------

    private final ConcurrentHashMap<String, LongAdder> personaTotal   = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> personaSuccess = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> personaFail    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> personaRespMs  = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, LongAdder> stepTotal   = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> stepSuccess = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> stepFail    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> stepRespMs  = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Step key intern cache
    // -------------------------------------------------------------------------

    private final ConcurrentHashMap<String, String> stepKeyCache =
            new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Histogram-based percentile tracking
    // -------------------------------------------------------------------------

    private static final int MAX_HISTO_MS    = 120_000;
    private static final int OVERFLOW_BUCKET = MAX_HISTO_MS;
    private static final int HISTO_SIZE      = MAX_HISTO_MS + 1;

    private final AtomicLongArray globalHisto = buildHisto();

    private final ConcurrentHashMap<String, AtomicLongArray> personaHisto =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, AtomicLongArray> stepHisto =
            new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Raw metric circular buffer
    // -------------------------------------------------------------------------

    private static final int RAW_SIZE = 100_000;

    private final RequestMetric[] rawRing = new RequestMetric[RAW_SIZE];

    private final AtomicLong rawIndex = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Successful response TPS window
    // -------------------------------------------------------------------------

    private static final int TPS_WINDOW_SIZE = 100_000;

    private final AtomicLongArray tpsRing =
            new AtomicLongArray(TPS_WINDOW_SIZE);

    private final AtomicLong tpsIndex =
            new AtomicLong(0);

    // =========================================================================
    // Request-sent tracking
    // =========================================================================

    /**
     * Call this immediately before the HTTP write when {@link #isTrackSentRpsEnabled()} is true.
     */
    public void recordRequestSent() {
        if (!trackSentRps) {
            return;
        }

        long now = System.currentTimeMillis();

        totalRequestsSent.increment();

        long currentSecond = now / 1000;

        requestsSentPerSecond
                .computeIfAbsent(currentSecond, ignored -> new LongAdder())
                .increment();

        sentRequestTimeRing.set(
                (int) (sentRequestIndex.getAndIncrement() % SENT_RPS_WINDOW_SIZE),
                now
        );
    }

    public void setDetailedRequestLogEnabled(boolean enabled) {
        this.recordDetailedRequests = enabled;
    }

    public boolean isDetailedRequestLogEnabled() {
        return recordDetailedRequests;
    }

    public void setTrackSentRpsEnabled(boolean enabled) {
        this.trackSentRps = enabled;
    }

    public boolean isTrackSentRpsEnabled() {
        return trackSentRps;
    }

    /**
     * Pre-create per-persona and per-step counters to avoid {@code computeIfAbsent} on the hot path.
     */
    public void registerPersonas(List<Persona> personas) {
        for (Persona persona : personas) {
            String personaName = persona.name;
            ensurePersonaAdders(personaName);
            personaHisto.putIfAbsent(personaName, buildHisto());

            for (ApiStep step : persona.steps) {
                String key = stepKey(personaName, step.name);
                ensureStepAdders(key);
                stepHisto.putIfAbsent(key, buildHisto());
            }
        }
    }

    private void ensurePersonaAdders(String personaName) {
        adder(personaTotal, personaName);
        adder(personaSuccess, personaName);
        adder(personaFail, personaName);
        adder(personaRespMs, personaName);
    }

    private void ensureStepAdders(String key) {
        adder(stepTotal, key);
        adder(stepSuccess, key);
        adder(stepFail, key);
        adder(stepRespMs, key);
    }

    public long getTotalRequestsSent() {
        return totalRequestsSent.sum();
    }

    public long getMaxRequestsSentPerSecond() {
        long max = 0;

        for (LongAdder count : requestsSentPerSecond.values()) {
            max = Math.max(max, count.sum());
        }

        return max;
    }

    public double getAverageRequestsSentPerSecond() {
        if (requestsSentPerSecond.isEmpty()) {
            return 0;
        }

        long total = 0;

        for (LongAdder count : requestsSentPerSecond.values()) {
            total += count.sum();
        }

        return (double) total / requestsSentPerSecond.size();
    }

    /**
     * Returns total requests sent by all users in the last 1 second.
     */
    public double getCurrentRequestsSentPerSecond() {
        long cutoff = System.currentTimeMillis() - 1_000;

        int count = 0;
        int size = (int) Math.min(sentRequestIndex.get(), SENT_RPS_WINDOW_SIZE);

        for (int i = 0; i < size; i++) {
            if (sentRequestTimeRing.get(i) >= cutoff) {
                count++;
            }
        }

        return count;
    }

    // =========================================================================
    // Hot path for completed request metrics
    // =========================================================================

    /**
     * Call this AFTER response/failure is available.
     */
    public void add(RequestMetric metric) {
        totalRequests.increment();
        totalResponseMs.add(metric.responseTimeMs);

        if (metric.success) {
            successCount.increment();
        } else {
            failureCount.increment();
        }

        recordFailedStatusCode(metric);

        updateMin(metric.responseTimeMs);
        updateMax(metric.responseTimeMs);

        histoRecord(globalHisto, metric.responseTimeMs);

        String p = metric.personaName == null ? "UNKNOWN_PERSONA" : metric.personaName;
        String s = metric.stepName == null ? "UNKNOWN_STEP" : metric.stepName;

        LongAdder personaTotalAdder = personaTotal.get(p);
        if (personaTotalAdder != null) {
            personaTotalAdder.increment();
            personaRespMs.get(p).add(metric.responseTimeMs);
            if (metric.success) {
                personaSuccess.get(p).increment();
            } else {
                personaFail.get(p).increment();
            }
        } else {
            adder(personaTotal, p).increment();
            adder(personaRespMs, p).add(metric.responseTimeMs);
            if (metric.success) {
                adder(personaSuccess, p).increment();
            } else {
                adder(personaFail, p).increment();
            }
        }

        boolean sampleDetailHisto = shouldSampleDetailHisto();
        if (sampleDetailHisto) {
            AtomicLongArray personaHistogram = personaHisto.get(p);
            if (personaHistogram != null) {
                histoRecord(personaHistogram, metric.responseTimeMs);
            }
        }

        String key = stepKeyCache.computeIfAbsent(
                // Null char separator avoids accidental collision with persona/step names.
                p + '\u0000' + s,
                k -> k
        );

        LongAdder stepTotalAdder = stepTotal.get(key);
        if (stepTotalAdder != null) {
            stepTotalAdder.increment();
            stepRespMs.get(key).add(metric.responseTimeMs);
            if (metric.success) {
                stepSuccess.get(key).increment();
            } else {
                stepFail.get(key).increment();
            }
        } else {
            adder(stepTotal, key).increment();
            adder(stepRespMs, key).add(metric.responseTimeMs);
            if (metric.success) {
                adder(stepSuccess, key).increment();
            } else {
                adder(stepFail, key).increment();
            }
        }

        if (sampleDetailHisto) {
            AtomicLongArray stepHistogram = stepHisto.get(key);
            if (stepHistogram != null) {
                histoRecord(stepHistogram, metric.responseTimeMs);
            }
        }

        if (recordDetailedRequests) {
            rawRing[(int) (rawIndex.getAndIncrement() % RAW_SIZE)] = metric;
        }

        if (metric.success) {
            liveSuccessCounter.increment();
        }
    }

    private boolean shouldSampleDetailHisto() {
        long total = totalRequests.sum();
        if (total < HISTO_SAMPLE_MIN_REQUESTS) {
            return true;
        }
        // Sample 1 in 4 persona/step histogram updates under heavy load.
        return (total & 3) == 0;
    }

    // =========================================================================
    // Failed status-code accessors
    // =========================================================================

    private void recordFailedStatusCode(RequestMetric metric) {
        if (metric.success) {
            return;
        }

        failedStatusCodeCounts
                .computeIfAbsent(metric.statusCode, ignored -> new LongAdder())
                .increment();
    }

    public long getFailedStatusCount(int statusCode) {
        LongAdder count = failedStatusCodeCounts.get(statusCode);
        return count == null ? 0 : count.sum();
    }

    public long getFailedStatus0Count() {
        return getFailedStatusCount(0);
    }

    public long getFailedStatus400Count() {
        return getFailedStatusCount(400);
    }

    public long getFailedStatus401Count() {
        return getFailedStatusCount(401);
    }

    public long getFailedStatus403Count() {
        return getFailedStatusCount(403);
    }

    public long getFailedStatus404Count() {
        return getFailedStatusCount(404);
    }

    public long getFailedStatus429Count() {
        return getFailedStatusCount(429);
    }

    public long getFailedStatus500Count() {
        return getFailedStatusCount(500);
    }

    public long getFailedStatus502Count() {
        return getFailedStatusCount(502);
    }

    public long getFailedStatus503Count() {
        return getFailedStatusCount(503);
    }

    public long getFailedStatus504Count() {
        return getFailedStatusCount(504);
    }

    public long getOtherFailedStatusCount() {
        long total = 0;

        for (Map.Entry<Integer, LongAdder> entry : failedStatusCodeCounts.entrySet()) {
            int statusCode = entry.getKey();

            if (!KNOWN_FAILED_STATUS_CODES.contains(statusCode)) {
                total += entry.getValue().sum();
            }
        }

        return total;
    }

    public Map<Integer, Long> getFailedStatusCodeCounts() {
        Map<Integer, Long> result = new TreeMap<>();

        for (Map.Entry<Integer, LongAdder> entry : failedStatusCodeCounts.entrySet()) {
            result.put(entry.getKey(), entry.getValue().sum());
        }

        return result;
    }

    // =========================================================================
    // Global accessors
    // =========================================================================

    public long getTotalRequests() {
        return totalRequests.sum();
    }

    public long getSuccessCount() {
        return successCount.sum();
    }

    public long getFailureCount() {
        return failureCount.sum();
    }

    public long getMinResponseTime() {
        long value = minResponseMs.get();
        return value == Long.MAX_VALUE ? 0 : value;
    }

    public long getMaxResponseTime() {
        return maxResponseMs.get();
    }

    public double getAverageResponseTime() {
        long total = totalRequests.sum();
        return total == 0 ? 0 : (double) totalResponseMs.sum() / total;
    }

    public double getErrorRate() {
        long total = totalRequests.sum();
        return total == 0 ? 0 : (failureCount.sum() * 100.0) / total;
    }

    public long getPercentileResponseTime(double percentile) {
        return histoPercentile(globalHisto, percentile);
    }

    // =========================================================================
    // Per-persona accessors
    // =========================================================================

    public long getTotalRequestsForPersona(String persona) {
        return adderSum(personaTotal, persona);
    }

    public long getSuccessCountForPersona(String persona) {
        return adderSum(personaSuccess, persona);
    }

    public long getFailureCountForPersona(String persona) {
        return adderSum(personaFail, persona);
    }

    public double getAverageResponseTimeForPersona(String persona) {
        long total = adderSum(personaTotal, persona);
        return total == 0 ? 0 : (double) adderSum(personaRespMs, persona) / total;
    }

    public long getPercentileResponseTimeForPersona(String persona, double percentile) {
        AtomicLongArray histo = personaHisto.get(persona);
        return histo == null ? 0 : histoPercentile(histo, percentile);
    }

    // =========================================================================
    // Per-step accessors
    // =========================================================================

    public long getTotalRequestsForStep(String persona, String step) {
        return adderSum(stepTotal, stepKey(persona, step));
    }

    public long getSuccessCountForStep(String persona, String step) {
        return adderSum(stepSuccess, stepKey(persona, step));
    }

    public long getFailureCountForStep(String persona, String step) {
        return adderSum(stepFail, stepKey(persona, step));
    }

    public double getAverageResponseTimeForStep(String persona, String step) {
        String key = stepKey(persona, step);
        long total = adderSum(stepTotal, key);

        return total == 0 ? 0 : (double) adderSum(stepRespMs, key) / total;
    }

    public long getPercentileResponseTimeForStep(
            String persona,
            String step,
            double percentile
    ) {
        AtomicLongArray histo = stepHisto.get(stepKey(persona, step));
        return histo == null ? 0 : histoPercentile(histo, percentile);
    }

    // =========================================================================
    // Successful response TPS
    // =========================================================================

    /**
     * Successful responses per second in the last 1-second window.
     * Only requests matching the step's expected status code are counted.
     */
    public double getCurrentTps() {
        return getCurrentSuccessfulResponsesPerSecond();
    }

    public double getCurrentSuccessfulResponsesPerSecond() {
        long cutoff = System.currentTimeMillis() - 1_000;

        int count = 0;
        int size = (int) Math.min(tpsIndex.get(), TPS_WINDOW_SIZE);

        for (int i = 0; i < size; i++) {
            if (tpsRing.get(i) >= cutoff) {
                count++;
            }
        }

        return count;
    }

    /**
     * O(1) live TPS for periodic reporting — resets the counter each call.
     */
    public double getAndResetLiveSuccessfulTps(long windowMs) {
        if (windowMs <= 0) {
            return 0;
        }
        long count = liveSuccessCounter.sumThenReset();
        return count * 1000.0 / windowMs;
    }

    // =========================================================================
    // Raw metrics
    // =========================================================================

    public List<RequestMetric> getAllMetrics() {
        List<RequestMetric> result = new ArrayList<>(RAW_SIZE);

        for (RequestMetric metric : rawRing) {
            if (metric != null) {
                result.add(metric);
            }
        }

        return result;
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    private static AtomicLongArray buildHisto() {
        return new AtomicLongArray(HISTO_SIZE);
    }

    private static void histoRecord(AtomicLongArray histo, long ms) {
        int bucket = ms >= MAX_HISTO_MS ? OVERFLOW_BUCKET : (int) Math.max(0, ms);
        histo.incrementAndGet(bucket);
    }

    private static long histoPercentile(AtomicLongArray histo, double percentile) {
        long total = 0;

        for (int i = 0; i < HISTO_SIZE; i++) {
            total += histo.get(i);
        }

        if (total == 0) {
            return 0;
        }

        long target = (long) Math.ceil((percentile / 100.0) * total);
        long running = 0;

        for (int i = 0; i < HISTO_SIZE; i++) {
            running += histo.get(i);

            if (running >= target) {
                return i;
            }
        }

        return MAX_HISTO_MS;
    }

    private LongAdder adder(ConcurrentHashMap<String, LongAdder> map, String key) {
        return map.computeIfAbsent(key, ignored -> new LongAdder());
    }

    private long adderSum(ConcurrentHashMap<String, LongAdder> map, String key) {
        LongAdder adder = map.get(key);
        return adder == null ? 0 : adder.sum();
    }

    private String stepKey(String persona, String step) {
        String safePersona = persona == null ? "UNKNOWN_PERSONA" : persona;
        String safeStep = step == null ? "UNKNOWN_STEP" : step;

        String lookupKey = safePersona + '\u0000' + safeStep;

        return stepKeyCache.getOrDefault(lookupKey, lookupKey);
    }

    private void updateMin(long value) {
        long current;

        do {
            current = minResponseMs.get();

            if (value >= current) {
                return;
            }
        } while (!minResponseMs.compareAndSet(current, value));
    }

    private void updateMax(long value) {
        long current;

        do {
            current = maxResponseMs.get();

            if (value <= current) {
                return;
            }
        } while (!maxResponseMs.compareAndSet(current, value));
    }
}
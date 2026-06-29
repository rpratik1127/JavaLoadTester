package org.tester.report;

import org.tester.executor.HttpExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.metrics.PhaseTimingSummary;
import org.tester.model.Persona;

import java.util.List;

/**
 * Prints terminal summary and appends one row to the CSV summary via {@link CsvReportGenerator}.
 * <p>
 * All totals come from {@link MetricsCollector}; throughput uses successful responses only.
 */
public class ReportGenerator {

    public void printSummary(
            MetricsCollector metricsCollector,
            int durationSeconds,
            List<Persona> personas
    ) {
        long total = metricsCollector.getTotalRequests();
        long success = metricsCollector.getSuccessCount();
        long failures = metricsCollector.getFailureCount();
        double errRate = metricsCollector.getErrorRate();
        double avgResp = metricsCollector.getAverageResponseTime();
        double tps = success / (double) durationSeconds;

        SentRateStats sentRateStats = resolveSentRateStats(metricsCollector, tps);

        long minRt = metricsCollector.getMinResponseTime();
        long maxRt = metricsCollector.getMaxResponseTime();
        long p90 = metricsCollector.getPercentileResponseTime(90);
        long p95 = metricsCollector.getPercentileResponseTime(95);
        long p99 = metricsCollector.getPercentileResponseTime(99);
        long p999 = metricsCollector.getPercentileResponseTime(99.9);

        printGlobalSummary(
                durationSeconds, total, success, failures, errRate, tps, sentRateStats,
                avgResp, minRt, maxRt, p90, p95, p99, p999
        );
        printPersonaSummaries(metricsCollector, personas);
        printStepSummaries(metricsCollector, personas);
        printPhaseTimingSummary(metricsCollector);
        printConnectionMetrics(metricsCollector);

        appendCsvSummary(
                metricsCollector,
                durationSeconds,
                total,
                success,
                failures,
                errRate,
                tps,
                sentRateStats,
                avgResp,
                minRt,
                maxRt,
                p90,
                p95,
                p99,
                p999
        );

        System.out.println("\n=============================");
    }

    private static SentRateStats resolveSentRateStats(MetricsCollector metricsCollector, double tps) {
        if (metricsCollector.isTrackSentRpsEnabled()) {
            return new SentRateStats(
                    metricsCollector.getMaxRequestsSentPerSecond(),
                    metricsCollector.getAverageRequestsSentPerSecond()
            );
        }
        // When send-time tracking is off, completed-response throughput is the best available proxy.
        return new SentRateStats(tps, tps);
    }

    private static void printGlobalSummary(
            int durationSeconds,
            long total,
            long success,
            long failures,
            double errRate,
            double tps,
            SentRateStats sentRateStats,
            double avgResp,
            long minRt,
            long maxRt,
            long p90,
            long p95,
            long p99,
            long p999
    ) {
        System.out.println("\n===== Load Test Summary =====");
        System.out.printf("Duration              : %d sec%n", durationSeconds);
        System.out.printf("Total Requests        : %d%n", total);
        System.out.printf("Success               : %d%n", success);
        System.out.printf("Failures              : %d%n", failures);
        System.out.printf("Error Rate            : %.2f%%%n", errRate);
        System.out.printf("Throughput            : %.2f req/sec%n", tps);
        System.out.printf("Max Req Sent/sec      : %.2f%n", sentRateStats.maxPerSec);
        System.out.printf("Avg Req Sent/sec      : %.2f%n", sentRateStats.avgPerSec);

        System.out.println();
        System.out.printf("Avg Response Time     : %.2f ms%n", avgResp);
        System.out.printf("Min Response Time     : %d ms%n", minRt);
        System.out.printf("Max Response Time     : %d ms%n", maxRt);
        System.out.printf("P90                   : %d ms%n", p90);
        System.out.printf("P95                   : %d ms%n", p95);
        System.out.printf("P99                   : %d ms%n", p99);
        System.out.printf("P99.9                 : %d ms%n", p999);
    }

    private static void printPersonaSummaries(MetricsCollector metricsCollector, List<Persona> personas) {
        System.out.println("\n===== Per Persona Summary =====");
        for (Persona persona : personas) {
            long pTotal = metricsCollector.getTotalRequestsForPersona(persona.name);
            if (pTotal == 0) {
                continue;
            }

            long pFail = metricsCollector.getFailureCountForPersona(persona.name);

            System.out.println();
            System.out.printf("Persona           : %s%n", persona.name);
            System.out.printf("Total Requests    : %d%n", pTotal);
            System.out.printf("Success           : %d%n", metricsCollector.getSuccessCountForPersona(persona.name));
            System.out.printf("Failures          : %d%n", pFail);
            System.out.printf("Avg Response Time : %.2f ms%n", metricsCollector.getAverageResponseTimeForPersona(persona.name));
            System.out.printf("Error Rate        : %.2f%%%n", (pFail * 100.0) / pTotal);
        }
    }

    private static void printStepSummaries(MetricsCollector metricsCollector, List<Persona> personas) {
        System.out.println("\n===== Per Step Summary =====");
        for (Persona persona : personas) {
            for (var step : persona.steps) {
                long sTotal = metricsCollector.getTotalRequestsForStep(persona.name, step.name);
                if (sTotal == 0) {
                    continue;
                }

                long sFail = metricsCollector.getFailureCountForStep(persona.name, step.name);

                System.out.println();
                System.out.printf("Step              : %s / %s%n", persona.name, step.name);
                System.out.printf("Total Requests    : %d%n", sTotal);
                System.out.printf("Success           : %d%n", metricsCollector.getSuccessCountForStep(persona.name, step.name));
                System.out.printf("Failures          : %d%n", sFail);
                System.out.printf("Avg Response Time : %.2f ms%n", metricsCollector.getAverageResponseTimeForStep(persona.name, step.name));
                System.out.printf("Error Rate        : %.2f%%%n", (sFail * 100.0) / sTotal);
            }
        }
    }

    private static void appendCsvSummary(
            MetricsCollector metricsCollector,
            int durationSeconds,
            long total,
            long success,
            long failures,
            double errRate,
            double tps,
            SentRateStats sentRateStats,
            double avgResp,
            long minRt,
            long maxRt,
            long p90,
            long p95,
            long p99,
            long p999
    ) {
        CsvReportGenerator.appendSummary(
                durationSeconds,
                total,
                success,
                failures,
                errRate,
                tps,
                sentRateStats.maxPerSec,
                sentRateStats.avgPerSec,
                avgResp,
                minRt,
                maxRt,
                p90,
                p95,
                p99,
                p999,
                metricsCollector.getFailedStatus0Count(),
                metricsCollector.getFailedStatus400Count(),
                metricsCollector.getFailedStatus401Count(),
                metricsCollector.getFailedStatus403Count(),
                metricsCollector.getFailedStatus404Count(),
                metricsCollector.getFailedStatus429Count(),
                metricsCollector.getFailedStatus500Count(),
                metricsCollector.getFailedStatus502Count(),
                metricsCollector.getFailedStatus503Count(),
                metricsCollector.getFailedStatus504Count(),
                metricsCollector.getOtherFailedStatusCount()
        );
    }

    private static void printPhaseTimingSummary(MetricsCollector metricsCollector) {
        PhaseTimingSummary phases = metricsCollector.getPhaseTimingSummary();
        if (phases.sampleCount() == 0) {
            return;
        }

        System.out.println("\n===== Request Phase Timings (avg ms) =====");
        System.out.printf("Samples               : %d%n", phases.sampleCount());
        System.out.printf("Created -> AcquireStart: %.2f%n", phases.toAcquireStartMs());
        System.out.printf("AcquireStart -> Acquired: %.2f%n", phases.acquireMs());
        System.out.printf("Acquired -> Pre-Write   : %.2f  (in-flight + event-loop queue)%n", phases.queueToWriteMs());
        System.out.printf("Pre-Write -> WriteDone  : %.2f%n", phases.writeMs());
        System.out.printf("WriteDone -> FirstByte  : %.2f  (server TTFB + network)%n", phases.serverTtfbMs());
        System.out.printf("FirstByte -> FirstChunk: %.2f  (body stream starts on event loop)%n",
                phases.headerToFirstChunkMs());
        System.out.printf("FirstChunk -> LastRead : %.2f  (body on wire, event-loop reads)%n",
                phases.bodyOnWireMs());
        System.out.printf("LastRead -> Aggregated : %.2f  (event-loop backlog after bytes received)%n",
                phases.aggregateWaitMs());
        System.out.printf("FirstByte -> FullBody   : %.2f  (total body phase)%n", phases.bodyMs());
        System.out.printf("Created -> FullBody     : %.2f%n", phases.totalMs());
    }

    private static void printConnectionMetrics(MetricsCollector metricsCollector) {
        System.out.println("\n===== Connection Metrics =====");
        System.out.printf("Active channels       : %d%n", HttpExecutor.getActiveChannelCount());
        System.out.printf("Requests / connection : %.2f%n", HttpExecutor.getRequestsPerConnection());
        System.out.printf("Connection reuse rate : %.1f%%%n", HttpExecutor.getConnectionReuseRate() * 100.0);
        System.out.printf("In-flight HTTP ops    : %d%n", HttpExecutor.getInFlightRequestCount());
        System.out.printf("Latency P50           : %d ms%n", metricsCollector.getPercentileResponseTime(50));
        System.out.printf("Latency P90           : %d ms%n", metricsCollector.getPercentileResponseTime(90));
        System.out.printf("Latency P99           : %d ms%n", metricsCollector.getPercentileResponseTime(99));
    }

    private record SentRateStats(double maxPerSec, double avgPerSec) {
    }
}

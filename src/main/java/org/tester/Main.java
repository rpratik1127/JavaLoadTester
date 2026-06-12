package org.tester;

import org.tester.config.CliOptions;
import org.tester.config.TestConstants;
import org.tester.control.ThroughputController;
import org.tester.executor.ConnectionMode;
import org.tester.executor.HttpExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.model.TestPlan;
import org.tester.parser.PersonaParser;
import org.tester.report.CsvReportGenerator;
import org.tester.report.ReportGenerator;
import org.tester.report.RequestModeCompletionReporter;
import org.tester.runner.ConcurrentPersonaRunner;
import org.tester.runner.LoadTestShutdown;
import org.tester.selector.LoadInputMode;
import org.tester.selector.PersonaLoadConfig;
import org.tester.selector.PersonaUserSelector;
import org.tester.selector.RampUpSelector;
import org.tester.selector.TargetTpsSelector;
import org.tester.selector.TestDurationSelector;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Entry point for the Netty-based load tester.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // --- Setup ---
        CliOptions options = CliOptions.fromArgs(args);
        configureHttp(options.connectionMode);

        TestPlan testPlan = new PersonaParser().parse(TestConstants.DEFAULT_PERSONA_FILE);
        PersonaLoadConfig loadConfig = new PersonaUserSelector().selectLoadConfig(testPlan.personas);

        int durationSeconds = new TestDurationSelector().selectDurationInSeconds();
        int rampUpSeconds = new RampUpSelector().selectRampUpSeconds(durationSeconds);
        ThroughputController throughputController = createThroughputController();

        MetricsCollector metricsCollector = createMetricsCollector(testPlan, options);
        ConcurrentPersonaRunner runner = new ConcurrentPersonaRunner();
        ScheduledExecutorService liveReporter = startLiveReporter(metricsCollector);

        long startTimeMillis = System.currentTimeMillis();

        // --- Execute load test ---
        try {
            runner.runPersonas(
                    testPlan.personas,
                    loadConfig,
                    durationSeconds,
                    rampUpSeconds,
                    metricsCollector,
                    throughputController
            );
        } finally {
            shutdownLiveReporter(liveReporter);
            shutdownThroughputController(throughputController);
        }

        // --- Finalize counts and reports (single metrics snapshot) ---
        LoadTestShutdown.drainInflightHttp();

        if (loadConfig.mode == LoadInputMode.REQUESTS) {
            RequestModeCompletionReporter.print(metricsCollector, loadConfig, testPlan.personas);
        }

        int actualDurationSec = (int) ((System.currentTimeMillis() - startTimeMillis) / 1000);
        writeReports(metricsCollector, actualDurationSec, testPlan);

        HttpExecutor.shutdown();
    }

    private static void configureHttp(ConnectionMode connectionMode) {
        HttpExecutor.setConnectionMode(connectionMode);
        System.out.printf(
                "[Main] connection mode: %s (use --sticky-connections for per-user serialized channels)%n",
                connectionMode
        );
    }

    private static ThroughputController createThroughputController() {
        int targetTps = new TargetTpsSelector().selectTargetTps();
        return targetTps > 0 ? new ThroughputController(targetTps) : null;
    }

    private static MetricsCollector createMetricsCollector(TestPlan testPlan, CliOptions options) {
        MetricsCollector metricsCollector = new MetricsCollector();
        metricsCollector.registerPersonas(testPlan.personas);
        metricsCollector.setDetailedRequestLogEnabled(options.generateRequestLog);
        metricsCollector.setTrackSentRpsEnabled(options.trackSentRps);

        if (!options.generateRequestLog) {
            System.out.println("[Main] request log disabled (--no-request-log)");
        }

        return metricsCollector;
    }

    private static ScheduledExecutorService startLiveReporter(MetricsCollector metricsCollector) {
        ScheduledExecutorService liveReporter = Executors.newSingleThreadScheduledExecutor();
        liveReporter.scheduleAtFixedRate(
                () -> System.out.printf(
                        "[LIVE] TPS: %.0f | Total: %d | Errors: %.1f%%%n",
                        metricsCollector.getAndResetLiveSuccessfulTps(TestConstants.LIVE_REPORT_INTERVAL_MS),
                        metricsCollector.getTotalRequests(),
                        metricsCollector.getErrorRate()
                ),
                TestConstants.LIVE_REPORT_INTERVAL_MS,
                TestConstants.LIVE_REPORT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        return liveReporter;
    }

    private static void shutdownLiveReporter(ScheduledExecutorService liveReporter) {
        liveReporter.shutdownNow();
    }

    private static void shutdownThroughputController(ThroughputController throughputController) {
        if (throughputController != null) {
            throughputController.shutdown();
        }
    }

    private static void writeReports(
            MetricsCollector metricsCollector,
            int actualDurationSec,
            TestPlan testPlan
    ) throws Exception {
        new ReportGenerator().printSummary(metricsCollector, actualDurationSec, testPlan.personas);

        CsvReportGenerator csvReportGenerator = new CsvReportGenerator();
        csvReportGenerator.generateStepReport(
                metricsCollector, testPlan.personas, TestConstants.STEP_REPORT_FILE);
        csvReportGenerator.generateDetailedRequestReport(
                metricsCollector, TestConstants.REQUEST_LOG_FILE);
    }
}

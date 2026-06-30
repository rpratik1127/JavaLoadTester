package org.tester.api.service;

import org.springframework.stereotype.Service;
import org.tester.api.dto.LoadTestRequest;
import org.tester.api.dto.LoadTestResponse;
import org.tester.api.dto.LoadTestStatus;
import org.tester.api.dto.PersonaMetricsDto;
import org.tester.config.TestConstants;
import org.tester.control.ThroughputController;
import org.tester.executor.ConnectionMode;
import org.tester.executor.HttpExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.model.Persona;
import org.tester.model.TestPlan;
import org.tester.parser.PersonaParser;
import org.tester.report.CsvReportGenerator;
import org.tester.report.ReportGenerator;
import org.tester.report.RequestModeCompletionReporter;
import org.tester.runner.ConcurrentPersonaRunner;
import org.tester.runner.LoadTestShutdown;
import org.tester.selector.LoadInputMode;
import org.tester.selector.PersonaLoadConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates load-test execution, reporting, and execution state for CLI and REST entry points.
 */
@Service
public class LoadTestService {

    private final PersonaParser personaParser;
    private final ReportGenerator reportGenerator;
    private final CsvReportGenerator csvReportGenerator;
    private final ConcurrentPersonaRunner runner;
    private final LoadTestExecutionStore executionStore;

    /** Injects parsers, reporters, runner, and the in-memory execution store. */
    public LoadTestService(
            PersonaParser personaParser,
            ReportGenerator reportGenerator,
            CsvReportGenerator csvReportGenerator,
            ConcurrentPersonaRunner runner,
            LoadTestExecutionStore executionStore
    ) {
        this.personaParser = personaParser;
        this.reportGenerator = reportGenerator;
        this.csvReportGenerator = csvReportGenerator;
        this.runner = runner;
        this.executionStore = executionStore;
    }

    /** Validates the request, runs the test, and persists status through completion or failure. */
    public LoadTestResponse runTest(LoadTestRequest request) throws Exception {
        validateRequest(request);

        String executionId = UUID.randomUUID().toString();
        LoadTestResponse runningResponse = new LoadTestResponse();
        runningResponse.setExecutionId(executionId);
        runningResponse.setStatus(LoadTestStatus.RUNNING);
        executionStore.save(executionId, runningResponse);

        try {
            return executeLoadTest(request, executionId);
        } catch (Exception ex) {
            LoadTestResponse failedResponse = new LoadTestResponse();
            failedResponse.setExecutionId(executionId);
            failedResponse.setStatus(LoadTestStatus.FAILED);
            failedResponse.setErrorMessage(ex.getMessage());
            executionStore.save(executionId, failedResponse);
            throw ex;
        }
    }

    /** Looks up a prior execution snapshot by identifier. */
    public LoadTestResponse getExecution(String executionId) {
        return executionStore.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));
    }

    /** Runs personas, drains in-flight work, writes reports, and builds the API response. */
    private LoadTestResponse executeLoadTest(LoadTestRequest request, String executionId) throws Exception {
        ConnectionMode connectionMode = resolveConnectionMode(request);
        configureHttp(connectionMode);

        TestPlan testPlan = prepareTestPlan(request);
        PersonaLoadConfig loadConfig = buildLoadConfig(request);
        int durationSeconds = request.getDurationSeconds();
        int rampUpSeconds = request.getRampUpSeconds();
        ThroughputController throughputController = createThroughputController(request.getTargetTps());

        boolean generateRequestLog = Boolean.TRUE.equals(
                request.getGenerateRequestLog() != null ? request.getGenerateRequestLog() : Boolean.FALSE
        );
        boolean trackSentRps = Boolean.TRUE.equals(request.getTrackSentRps());
        boolean liveReportingEnabled = Boolean.TRUE.equals(request.getLiveReportingEnabled());

        String stepReportFile = resolveReportPath(request.getStepReportFile(), TestConstants.STEP_REPORT_FILE);
        String requestLogFile = resolveReportPath(request.getRequestLogFile(), TestConstants.REQUEST_LOG_FILE);

        MetricsCollector metricsCollector = createMetricsCollector(
                testPlan, generateRequestLog, trackSentRps, requestLogFile
        );
        ScheduledExecutorService liveReporter = liveReportingEnabled
                ? startLiveReporter(metricsCollector)
                : null;

        long startTimeMillis = System.currentTimeMillis();

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

        if (loadConfig.mode != LoadInputMode.REQUESTS) {
            LoadTestShutdown.drainInflightHttp();
        }

        if (loadConfig.mode == LoadInputMode.REQUESTS) {
            RequestModeCompletionReporter.print(metricsCollector, loadConfig, testPlan.personas);
        }

        int actualDurationSec = (int) ((System.currentTimeMillis() - startTimeMillis) / 1000);
        writeReports(metricsCollector, actualDurationSec, testPlan, stepReportFile, requestLogFile);

        HttpExecutor.shutdown();

        LoadTestResponse response = buildResponse(
                executionId,
                metricsCollector,
                actualDurationSec,
                stepReportFile,
                requestLogFile,
                trackSentRps,
                testPlan,
                loadConfig
        );
        executionStore.save(executionId, response);
        return response;
    }

    /** Rejects incomplete or inconsistent load-test configuration early. */
    private void validateRequest(LoadTestRequest request) {
        if (request.getPersona() == null) {
            throw new IllegalArgumentException("persona is required and must contain the full persona definition");
        }

        if (request.getPersona().personas == null || request.getPersona().personas.isEmpty()) {
            throw new IllegalArgumentException("persona.personas must contain at least one persona");
        }

        if (request.getLoadInputMode() == null) {
            throw new IllegalArgumentException("loadInputMode is required");
        }

        if (request.getValuesPerPersona() == null || request.getValuesPerPersona().isEmpty()) {
            throw new IllegalArgumentException("valuesPerPersona is required");
        }

        validateValuesPerPersona(request);

        if (request.getDurationSeconds() <= 0) {
            throw new IllegalArgumentException("durationSeconds must be greater than 0");
        }

        if (request.getRampUpSeconds() < 0) {
            throw new IllegalArgumentException("rampUpSeconds cannot be negative");
        }

        if (request.getRampUpSeconds() > request.getDurationSeconds()) {
            throw new IllegalArgumentException("rampUpSeconds cannot exceed durationSeconds");
        }

        if (request.getTargetTps() < 0) {
            throw new IllegalArgumentException("targetTps cannot be negative");
        }
    }

    /**
     * Ensures load values are provided for every persona and that at least one persona runs.
     * Mirrors the CLI {@link org.tester.selector.PersonaUserSelector} behavior.
     */
    private void validateValuesPerPersona(LoadTestRequest request) {
        Map<String, Integer> valuesPerPersona = request.getValuesPerPersona();
        var personaNames = request.getPersona().personas.stream()
                .map(persona -> persona.name)
                .collect(Collectors.toSet());

        if (personaNames.size() != request.getPersona().personas.size()) {
            throw new IllegalArgumentException("Persona names must be unique within persona.personas");
        }

        for (Persona persona : request.getPersona().personas) {
            if (persona.name == null || persona.name.isBlank()) {
                throw new IllegalArgumentException("Each persona must have a non-blank name");
            }
            if (!valuesPerPersona.containsKey(persona.name)) {
                throw new IllegalArgumentException(
                        "valuesPerPersona must include an entry for persona '" + persona.name
                                + "'. Available personas: " + personaNames
                );
            }
        }

        for (String key : valuesPerPersona.keySet()) {
            if (!personaNames.contains(key)) {
                throw new IllegalArgumentException(
                        "valuesPerPersona contains unknown persona '" + key
                                + "'. Available personas: " + personaNames
                );
            }
        }

        boolean hasPositiveLoad = valuesPerPersona.values().stream()
                .anyMatch(value -> value != null && value > 0);
        if (!hasPositiveLoad) {
            throw new IllegalArgumentException(
                    "At least one persona must have a positive load value in valuesPerPersona"
            );
        }
    }

    /** Applies static-body precomputation to inline persona JSON from the API request. */
    private TestPlan prepareTestPlan(LoadTestRequest request) throws Exception {
        return personaParser.prepare(request.getPersona());
    }

    /** Converts API load values into the immutable runner configuration. */
    private PersonaLoadConfig buildLoadConfig(LoadTestRequest request) {
        Map<String, Integer> valuesPerPersona = new LinkedHashMap<>();
        int totalUsers = 0;

        for (Map.Entry<String, Integer> entry : request.getValuesPerPersona().entrySet()) {
            int value = entry.getValue() != null ? entry.getValue() : 0;
            if (value < 0) {
                throw new IllegalArgumentException(
                        "valuesPerPersona value for " + entry.getKey() + " cannot be negative"
                );
            }
            if (value > 0) {
                valuesPerPersona.put(entry.getKey(), value);
                if (request.getLoadInputMode() == LoadInputMode.USERS) {
                    totalUsers += value;
                }
            }
        }

        if (request.getLoadInputMode() == LoadInputMode.USERS) {
            PersonaLoadConfig.setTotalUsers(totalUsers);
        }

        return new PersonaLoadConfig(request.getLoadInputMode(), valuesPerPersona);
    }

    private ConnectionMode resolveConnectionMode(LoadTestRequest request) {
        return request.getConnectionMode() != null ? request.getConnectionMode() : ConnectionMode.POOLED;
    }

    private String resolveReportPath(String override, String defaultPath) {
        return override != null && !override.isBlank() ? override : defaultPath;
    }

    private void configureHttp(ConnectionMode connectionMode) {
        HttpExecutor.setConnectionMode(connectionMode);
        System.out.printf(
                "[LoadTestService] connection mode: %s (use --sticky-connections for per-user serialized channels)%n",
                connectionMode
        );
    }

    private ThroughputController createThroughputController(int targetTps) {
        return targetTps > 0 ? new ThroughputController(targetTps) : null;
    }

    /** Creates the metrics collector for the current run and registers persona counters. */
    private MetricsCollector createMetricsCollector(
            TestPlan testPlan,
            boolean generateRequestLog,
            boolean trackSentRps,
            String requestLogFile
    ) {
        MetricsCollector metricsCollector = new MetricsCollector();
        metricsCollector.registerPersonas(testPlan.personas);
        metricsCollector.setDetailedRequestLogEnabled(generateRequestLog);
        metricsCollector.setTrackSentRpsEnabled(trackSentRps);

        if (!generateRequestLog) {
            System.out.println("[LoadTestService] request log disabled");
        } else {
            System.out.println("[LoadTestService] request log enabled -> " + requestLogFile);
        }

        return metricsCollector;
    }

    /** Starts periodic stdout reporting of live TPS, totals, and error rate. */
    private ScheduledExecutorService startLiveReporter(MetricsCollector metricsCollector) {
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

    private void shutdownLiveReporter(ScheduledExecutorService liveReporter) {
        if (liveReporter != null) {
            liveReporter.shutdownNow();
        }
    }

    private void shutdownThroughputController(ThroughputController throughputController) {
        if (throughputController != null) {
            throughputController.shutdown();
        }
    }

    /** Writes terminal summary, step CSV, and optional per-request log files. */
    private void writeReports(
            MetricsCollector metricsCollector,
            int actualDurationSec,
            TestPlan testPlan,
            String stepReportFile,
            String requestLogFile
    ) throws Exception {
        reportGenerator.printSummary(metricsCollector, actualDurationSec, testPlan.personas);
        csvReportGenerator.generateStepReport(metricsCollector, testPlan.personas, stepReportFile);
        csvReportGenerator.generateDetailedRequestReport(metricsCollector, requestLogFile);
    }

    /** Maps final metrics and report paths into the REST response DTO. */
    private LoadTestResponse buildResponse(
            String executionId,
            MetricsCollector metricsCollector,
            int actualDurationSec,
            String stepReportFile,
            String requestLogFile,
            boolean trackSentRps,
            TestPlan testPlan,
            PersonaLoadConfig loadConfig
    ) {
        long total = metricsCollector.getTotalRequests();
        long success = metricsCollector.getSuccessCount();
        long failures = metricsCollector.getFailureCount();
        double throughput = actualDurationSec > 0 ? success / (double) actualDurationSec : 0;

        LoadTestResponse response = new LoadTestResponse();
        response.setExecutionId(executionId);
        response.setStatus(LoadTestStatus.COMPLETED);
        response.setTotalRequests(total);
        response.setSuccessfulRequests(success);
        response.setFailedRequests(failures);
        response.setErrorRate(metricsCollector.getErrorRate());
        response.setThroughput(throughput);
        response.setAverageLatencyMs(metricsCollector.getAverageResponseTime());
        response.setExecutionDurationSeconds(actualDurationSec);
        response.setStepReportPath(stepReportFile);
        response.setRequestLogPath(requestLogFile);
        response.setMinLatencyMs(metricsCollector.getMinResponseTime());
        response.setMaxLatencyMs(metricsCollector.getMaxResponseTime());
        response.setP50LatencyMs(metricsCollector.getPercentileResponseTime(50));
        response.setP90LatencyMs(metricsCollector.getPercentileResponseTime(90));
        response.setP95LatencyMs(metricsCollector.getPercentileResponseTime(95));
        response.setP99LatencyMs(metricsCollector.getPercentileResponseTime(99));
        response.setP999LatencyMs(metricsCollector.getPercentileResponseTime(99.9));
        response.setFailedStatusCodeCounts(metricsCollector.getFailedStatusCodeCounts());

        if (trackSentRps) {
            response.setMaxRequestsSentPerSecond(metricsCollector.getMaxRequestsSentPerSecond());
            response.setAverageRequestsSentPerSecond(metricsCollector.getAverageRequestsSentPerSecond());
        } else {
            response.setMaxRequestsSentPerSecond(throughput);
            response.setAverageRequestsSentPerSecond(throughput);
        }

        response.setPerPersonaMetrics(buildPerPersonaMetrics(metricsCollector, testPlan, loadConfig));

        return response;
    }

    private Map<String, PersonaMetricsDto> buildPerPersonaMetrics(
            MetricsCollector metricsCollector,
            TestPlan testPlan,
            PersonaLoadConfig loadConfig
    ) {
        Map<String, PersonaMetricsDto> perPersona = new LinkedHashMap<>();

        for (Persona persona : testPlan.personas) {
            PersonaMetricsDto metrics = new PersonaMetricsDto();
            metrics.setPersonaName(persona.name);
            metrics.setConfiguredLoad(loadConfig.valuesPerPersona.getOrDefault(persona.name, 0));

            long personaTotal = metricsCollector.getTotalRequestsForPersona(persona.name);
            long personaSuccess = metricsCollector.getSuccessCountForPersona(persona.name);
            long personaFailures = metricsCollector.getFailureCountForPersona(persona.name);

            metrics.setTotalRequests(personaTotal);
            metrics.setSuccessfulRequests(personaSuccess);
            metrics.setFailedRequests(personaFailures);
            metrics.setErrorRate(personaTotal == 0 ? 0 : (personaFailures * 100.0) / personaTotal);
            metrics.setAverageLatencyMs(metricsCollector.getAverageResponseTimeForPersona(persona.name));

            perPersona.put(persona.name, metrics);
        }

        return perPersona;
    }
}

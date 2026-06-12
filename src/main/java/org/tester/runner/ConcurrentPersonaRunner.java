package org.tester.runner;

import org.tester.config.TestConstants;
import org.tester.control.RequestModePacer;
import org.tester.control.ThroughputController;
import org.tester.executor.StepExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.model.Persona;
import org.tester.selector.LoadInputMode;
import org.tester.selector.PersonaLoadConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Orchestrates load execution for all personas in either user mode or total-request mode.
 */
public class ConcurrentPersonaRunner {

    public void runPersonas(
            List<Persona> personas,
            PersonaLoadConfig loadConfig,
            int durationSeconds,
            int rampUpSeconds,
            MetricsCollector metricsCollector,
            ThroughputController throughputController
    ) throws InterruptedException {

        if (loadConfig.valuesPerPersona.isEmpty()) {
            System.out.println("No load configured. Load test will not run.");
            return;
        }

        StepExecutor stepExecutor = new StepExecutor(metricsCollector);
        long startTimeMillis = System.currentTimeMillis();
        long endTimeMillis = startTimeMillis + (durationSeconds * 1000L);
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

        RunContext context = new RunContext(
                personas,
                loadConfig,
                durationSeconds,
                rampUpSeconds,
                metricsCollector,
                throughputController,
                stepExecutor,
                executorService,
                startTimeMillis,
                endTimeMillis
        );

        if (loadConfig.mode == LoadInputMode.REQUESTS) {
            runRequestMode(context);
            return;
        }

        runUserMode(context);
    }

    private void runRequestMode(RunContext context) throws InterruptedException {
        RequestModePacer requestModePacer = new RequestModePacer(
                context.loadConfig.valuesPerPersona,
                context.startTimeMillis,
                context.durationSeconds
        );

        DynamicRequestModeScaler scaler = new DynamicRequestModeScaler(
                context.executorService,
                context.startTimeMillis,
                context.endTimeMillis,
                context.durationSeconds,
                context.rampUpSeconds,
                context.metricsCollector,
                context.stepExecutor,
                context.throughputController,
                requestModePacer,
                context.loadConfig.valuesPerPersona,
                context.personas
        );

        try {
            scaler.start();
            waitForRequestModeCompletion(
                    context.endTimeMillis,
                    requestModePacer,
                    context.loadConfig.valuesPerPersona,
                    context.personas
            );
        } finally {
            scaler.shutdown();
            LoadTestShutdown.drainExecutorAndInflightHttp(
                    context.executorService,
                    TestConstants.executorDrainSeconds()
            );

            PersonaLoadConfig.setSpawnedPerPersona(scaler.getSpawnedUsersSnapshot());
            PersonaLoadConfig.setTotalUsers(scaler.getTotalSpawned());
            requestModePacer.shutdown();
        }
    }

    private void runUserMode(RunContext context) throws InterruptedException {
        int totalUsers = PersonaLoadConfig.getTotalUsers();
        if (totalUsers == 0) {
            System.out.println("No load configured. Load test will not run.");
            return;
        }

        long delayBetweenUsersMs = computeRampDelayMs(context.rampUpSeconds, totalUsers);
        spawnUserModeVirtualUsers(context, delayBetweenUsersMs);

        LoadTestShutdown.drainExecutorAndInflightHttp(
                context.executorService,
                context.durationSeconds + context.rampUpSeconds + TestConstants.executorDrainSeconds()
        );
    }

    private static long computeRampDelayMs(int rampUpSeconds, int totalUsers) {
        if (rampUpSeconds <= 0 || totalUsers <= 1) {
            return 0;
        }
        return (rampUpSeconds * 1000L) / totalUsers;
    }

    private static void spawnUserModeVirtualUsers(RunContext context, long delayBetweenUsersMs)
            throws InterruptedException {
        for (Persona persona : context.personas) {
            int userCount = context.loadConfig.valuesPerPersona.getOrDefault(persona.name, 0);
            if (userCount == 0) {
                continue;
            }

            for (int localUserId = 1; localUserId <= userCount; localUserId++) {
                context.executorService.submit(
                        VirtualUser.forUserMode(
                                VirtualUserIds.format(persona.name, localUserId),
                                persona,
                                context.endTimeMillis,
                                context.metricsCollector,
                                context.stepExecutor,
                                context.throughputController
                        )
                );

                if (delayBetweenUsersMs > 0) {
                    Thread.sleep(delayBetweenUsersMs);
                }
            }
        }
    }

    /**
     * Blocks until end time or all persona budgets are fully consumed.
     * <p>
     * Uses {@link RequestModePacer#getConsumed()} (permits issued), not metrics,
     * because permits lead HTTP sends while responses may still be in flight.
     */
    private void waitForRequestModeCompletion(
            long endTimeMillis,
            RequestModePacer requestModePacer,
            Map<String, Integer> requestTargets,
            List<Persona> personas
    ) throws InterruptedException {
        while (System.currentTimeMillis() < endTimeMillis) {
            if (allRequestBudgetsMet(requestModePacer, requestTargets, personas)) {
                return;
            }
            Thread.sleep(TestConstants.REQUEST_MODE_POLL_MS);
        }

        Thread.sleep(TestConstants.REQUEST_MODE_END_DRAIN_MS);
    }

    private boolean allRequestBudgetsMet(
            RequestModePacer requestModePacer,
            Map<String, Integer> requestTargets,
            List<Persona> personas
    ) {
        for (Persona persona : personas) {
            int target = requestTargets.getOrDefault(persona.name, 0);
            if (target > 0 && requestModePacer.getConsumed(persona.name) < target) {
                return false;
            }
        }
        return true;
    }

    /**
     * Shared execution context passed between request-mode and user-mode runners.
     */
    private record RunContext(
            List<Persona> personas,
            PersonaLoadConfig loadConfig,
            int durationSeconds,
            int rampUpSeconds,
            MetricsCollector metricsCollector,
            ThroughputController throughputController,
            StepExecutor stepExecutor,
            ExecutorService executorService,
            long startTimeMillis,
            long endTimeMillis
    ) {
    }
}

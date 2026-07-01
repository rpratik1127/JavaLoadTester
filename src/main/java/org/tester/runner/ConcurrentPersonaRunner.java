package org.tester.runner;

import org.tester.config.TestConstants;
import org.tester.control.RequestModePacer;
import org.tester.control.ThroughputController;
import org.tester.executor.ConnectionMode;
import org.tester.executor.HttpExecutor;
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

    /** Executes the configured personas and records runtime metrics. */
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

    /** Runs total-request mode with dynamic VU scaling and budget pacing. */
    private void runRequestMode(RunContext context) throws InterruptedException {
        configurePooledConnectionsForRequestMode(context);
        warmRequestModePools(context);

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

    /** Spawns fixed virtual users per persona until the test window ends. */
    private void runUserMode(RunContext context) throws InterruptedException {
        configureConnectionsForUserMode();
        warmUserModePools(context);

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

    /** Enables dedicated keep-alive channels or sticky mode for user-mode throughput. */
    private static void configureConnectionsForUserMode() {
        if (HttpExecutor.getConnectionMode() == ConnectionMode.STICKY) {
            System.out.println(
                    "[ConcurrentPersonaRunner] User mode: sticky per-user connections (--sticky-connections)"
            );
            return;
        }
        HttpExecutor.setConnectionMode(ConnectionMode.POOLED);
        HttpExecutor.setDedicatedUserChannels(true);
        System.out.printf(
                "[ConcurrentPersonaRunner] User mode: dedicated keep-alive channel per VU "
                        + "(target %d TPS, pool cap %d/host, pipeline %d/conn)%n",
                TestConstants.TARGET_TPS,
                TestConstants.MAX_POOL_CONNECTIONS,
                TestConstants.MAX_PIPELINE_PER_CHANNEL
        );
    }

    /** Pre-opens pooled connections sized for each persona's virtual-user count. */
    private static void warmUserModePools(RunContext context) throws InterruptedException {
        if (HttpExecutor.getConnectionMode() == ConnectionMode.STICKY) {
            return;
        }
        for (Persona persona : context.personas) {
            int users = context.loadConfig.valuesPerPersona.getOrDefault(persona.name, 0);
            if (users <= 0) {
                continue;
            }
            int warmCount = Math.min(
                    TestConstants.POOL_WARMUP_MAX,
                    Math.max(
                            TestConstants.POOL_WARMUP_MIN,
                            Math.max(users, TestConstants.estimateUsersForRps(TestConstants.TARGET_TPS, 0.5))
                    )
            );
            warmPool(persona, warmCount);
        }
    }

    /** Sizes the shared pool for request-mode Little's-law throughput targets. */
    private static void configurePooledConnectionsForRequestMode(RunContext context) {
        HttpExecutor.setConnectionMode(ConnectionMode.POOLED);
        HttpExecutor.setDedicatedUserChannels(false);
        int poolSize = computeRequestModePoolSize(context);
        HttpExecutor.setMaxConnectionsPerHost(poolSize);
        System.out.printf(
                "[ConcurrentPersonaRunner] Request mode: shared pool up to %d conn/host "
                        + "(sized for target RPS × RTT at pipeline depth %d)%n",
                poolSize,
                TestConstants.MAX_PIPELINE_PER_CHANNEL
        );
    }

    /**
     * Little's law sizing: at pipeline depth 1 each connection carries one in-flight request,
     * so pool capacity must be at least targetRps × RTT to sustain the scheduled rate.
     */
    private static int computeRequestModePoolSize(RunContext context) {
        int maxNeeded = TestConstants.MAX_POOL_CONNECTIONS;
        for (Persona persona : context.personas) {
            int target = context.loadConfig.valuesPerPersona.getOrDefault(persona.name, 0);
            if (target <= 0) {
                continue;
            }
            int targetRps = RequestModePacer.computeTargetRps(target, context.durationSeconds);
            int needed = TestConstants.estimateUsersForRps(targetRps, 2.0);
            maxNeeded = Math.max(maxNeeded, needed);
        }
        return Math.min(maxNeeded, TestConstants.POOL_WARMUP_MAX);
    }

    /** Pre-opens pooled connections sized for each persona's scheduled request rate. */
    private static void warmRequestModePools(RunContext context) throws InterruptedException {
        int poolCap = HttpExecutor.getMaxConnectionsPerHost();
        for (Persona persona : context.personas) {
            int target = context.loadConfig.valuesPerPersona.getOrDefault(persona.name, 0);
            if (target <= 0) {
                continue;
            }
            int targetRps = RequestModePacer.computeTargetRps(target, context.durationSeconds);
            int warmCount = Math.min(
                    poolCap,
                    Math.max(
                            TestConstants.POOL_WARMUP_MIN,
                            TestConstants.estimateUsersForRps(targetRps, 1.0)
                    )
            );
            warmPool(persona, warmCount);
        }
    }

    /** Opens connections in batches; partial warmup is tolerated and logged. */
    private static void warmPool(Persona persona, int warmCount) throws InterruptedException {
        try {
            HttpExecutor.warmPool(persona.baseUrl, warmCount)
                    .get(120, java.util.concurrent.TimeUnit.SECONDS);
            System.out.printf(
                    "[ConcurrentPersonaRunner] Warmed %d pooled connection(s) for %s%n",
                    warmCount, persona.name
            );
        } catch (Exception e) {
            System.out.printf(
                    "[ConcurrentPersonaRunner] Pool warmup partial for %s: %s%n",
                    persona.name, e.getMessage()
            );
        }
    }

    /** Computes staggered spawn delay to spread virtual users across ramp-up. */
    private static long computeRampDelayMs(int rampUpSeconds, int totalUsers) {
        if (rampUpSeconds <= 0 || totalUsers <= 1) {
            return 0;
        }
        return (rampUpSeconds * 1000L) / totalUsers;
    }

    /** Distributes virtual users across personas with optional ramp-up pacing. */
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
        long budgetDrainDeadline = endTimeMillis + TestConstants.REQUEST_MODE_BUDGET_DRAIN_MS;

        while (true) {
            if (allRequestBudgetsMet(requestModePacer, requestTargets, personas)) {
                return;
            }
            if (System.currentTimeMillis() >= budgetDrainDeadline) {
                break;
            }
            Thread.sleep(TestConstants.REQUEST_MODE_POLL_MS);
        }

        Thread.sleep(TestConstants.REQUEST_MODE_END_DRAIN_MS);
    }

    /** True when every persona has consumed its full request budget. */
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

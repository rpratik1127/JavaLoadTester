package org.tester.runner;

import org.tester.control.PersonaRequestLimiter;
import org.tester.control.RequestModePacer;
import org.tester.control.ThroughputController;
import org.tester.executor.HttpExecutor;
import org.tester.executor.StepExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.metrics.RequestMetric;
import org.tester.model.ApiStep;
import org.tester.model.Persona;
import org.tester.runtime.VariableStore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one virtual user: loops through persona steps until time expires or the
 * request budget is exhausted. Each step is gated by optional TPS control and
 * budget acquisition before the HTTP send.
 * <p>
 * Uses a direct loop on virtual threads (blocking join on async HTTP futures)
 * instead of deep CompletableFuture chains — lower allocation and scheduling overhead.
 */
public class VirtualUser implements Runnable {

    private final String userId;
    private final Persona persona;
    private final long endTimeMillis;
    private final MetricsCollector metricsCollector;
    private final StepExecutor stepExecutor;
    private final ThroughputController throughputController;
    private final RequestModePacer requestModePacer;
    private final PersonaRequestLimiter requestLimiter;
    private final AtomicInteger activeUserCounter;
    private final AtomicBoolean keepAlive;

    /** Factory for fixed-duration user mode without request budgeting. */
    public static VirtualUser forUserMode(
            String userId,
            Persona persona,
            long endTimeMillis,
            MetricsCollector metricsCollector,
            StepExecutor stepExecutor,
            ThroughputController throughputController
    ) {
        return new VirtualUser(
                userId,
                persona,
                endTimeMillis,
                metricsCollector,
                stepExecutor,
                throughputController,
                null,
                null,
                null,
                null
        );
    }

    /** Factory for request-mode VUs gated by {@link RequestModePacer} permits. */
    public static VirtualUser forRequestMode(
            String userId,
            Persona persona,
            long endTimeMillis,
            MetricsCollector metricsCollector,
            StepExecutor stepExecutor,
            ThroughputController throughputController,
            RequestModePacer requestModePacer,
            AtomicInteger activeUserCounter
    ) {
        return new VirtualUser(
                userId,
                persona,
                endTimeMillis,
                metricsCollector,
                stepExecutor,
                throughputController,
                requestModePacer,
                null,
                activeUserCounter,
                null
        );
    }

    /** Internal constructor wiring optional pacing, budgeting, and lifecycle hooks. */
    VirtualUser(
            String userId,
            Persona persona,
            long endTimeMillis,
            MetricsCollector metricsCollector,
            StepExecutor stepExecutor,
            ThroughputController throughputController,
            RequestModePacer requestModePacer,
            PersonaRequestLimiter requestLimiter,
            AtomicInteger activeUserCounter,
            AtomicBoolean keepAlive
    ) {
        this.userId = userId;
        this.persona = persona;
        this.endTimeMillis = endTimeMillis;
        this.metricsCollector = metricsCollector;
        this.stepExecutor = stepExecutor;
        this.throughputController = throughputController;
        this.requestModePacer = requestModePacer;
        this.requestLimiter = requestLimiter;
        this.activeUserCounter = activeUserCounter;
        this.keepAlive = keepAlive;
    }

    @Override
    public void run() {
        try {
            VariableStore variableStore = new VariableStore();
            while (!shouldStop() && !isBudgetExhausted()) {
                runOneIteration(variableStore);
            }
        } finally {
            if (requestModePacer == null) {
                HttpExecutor.releaseDedicatedChannel(userId, persona.baseUrl);
            }
            if (activeUserCounter != null) {
                activeUserCounter.decrementAndGet();
            }
        }
    }

    /** Executes one full pass through the persona step list. */
    private void runOneIteration(VariableStore variableStore) {
        for (int stepIndex = 0; stepIndex < persona.steps.size(); stepIndex++) {
            if (shouldStop() || isBudgetExhausted()) {
                return;
            }

            ApiStep step = persona.steps.get(stepIndex);

            if (!acquireSendGateSync()) {
                return;
            }

            if (!acquireBudgetSync()) {
                return;
            }

            if (shouldStop()) {
                releaseBudget();
                return;
            }

            executeAndRecord(step, variableStore);
            sleepThinkTime(step);
        }
    }

    /** Blocks until global TPS throttling grants a send permit. */
    private boolean acquireSendGateSync() {
        if (throughputController == null) {
            return true;
        }
        throughputController.acquireAsync().join();
        return !shouldStop();
    }

    /** Acquires a persona request permit from the pacer or limiter before sending. */
    private boolean acquireBudgetSync() {
        if (requestModePacer != null) {
            if (requestModePacer.tryAcquireNow(persona.name)) {
                return true;
            }
            Boolean granted = requestModePacer.acquire(persona.name).join();
            return Boolean.TRUE.equals(granted);
        }
        if (requestLimiter != null) {
            return requestLimiter.tryAcquire(persona.name);
        }
        return true;
    }

    /** Sends one step and records success or failure in the metrics collector. */
    private void executeAndRecord(ApiStep step, VariableStore variableStore) {
        try {
            RequestMetric metric = stepExecutor
                    .executeAsync(userId, persona, step, variableStore)
                    .join();
            if (metric != null) {
                metricsCollector.add(metric);
            }
        } catch (Exception error) {
            metricsCollector.add(
                    RequestMetric.failed(userId, persona.name, step.name, error.getMessage())
            );
        }
    }

    /** Returns an unused request permit when the send is aborted after acquisition. */
    private void releaseBudget() {
        if (requestModePacer != null) {
            requestModePacer.release(persona.name);
        }
    }

    /** True when the persona has no remaining request permits. */
    private boolean isBudgetExhausted() {
        if (requestModePacer != null) {
            return requestModePacer.isExhausted(persona.name);
        }
        if (requestLimiter != null) {
            return requestLimiter.isExhausted(persona.name);
        }
        return false;
    }

    /** Applies configured think time between steps without blocking shutdown. */
    private static void sleepThinkTime(ApiStep step) {
        if (step.thinkTimeMs == null || step.thinkTimeMs <= 0) {
            return;
        }
        try {
            Thread.sleep(step.thinkTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops the VU when time expires, budget is exhausted, or keep-alive is cleared. */
    private boolean shouldStop() {
        if (keepAlive != null && !keepAlive.get()) {
            return true;
        }
        // Request mode: run until the budget is fully consumed, not just until end time.
        if (requestModePacer != null) {
            return requestModePacer.isExhausted(persona.name);
        }
        return System.currentTimeMillis() >= endTimeMillis;
    }
}

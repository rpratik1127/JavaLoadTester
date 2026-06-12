package org.tester.runner;

import org.tester.control.PersonaRequestLimiter;
import org.tester.control.RequestModePacer;
import org.tester.control.ThroughputController;
import org.tester.executor.StepExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.metrics.RequestMetric;
import org.tester.model.ApiStep;
import org.tester.model.Persona;
import org.tester.runtime.VariableStore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one virtual user: loops through persona steps until time expires or the
 * request budget is exhausted. Each step is gated by optional TPS control and
 * budget acquisition before the HTTP send.
 */
public class VirtualUser implements Runnable {

    /** Shared scheduler for think-time delays across all virtual users. */
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "async-vu-scheduler");
                t.setDaemon(true);
                return t;
            }
    );

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
            runIterations(variableStore).join();
        } finally {
            if (activeUserCounter != null) {
                activeUserCounter.decrementAndGet();
            }
        }
    }

    private CompletableFuture<Void> runIterations(VariableStore variableStore) {
        if (shouldStop() || isBudgetExhausted()) {
            return CompletableFuture.completedFuture(null);
        }

        return runAllSteps(0, variableStore)
                .thenCompose(ignored -> runIterations(variableStore));
    }

    private CompletableFuture<Void> runAllSteps(int stepIndex, VariableStore variableStore) {
        if (shouldStop() || isBudgetExhausted() || stepIndex >= persona.steps.size()) {
            return CompletableFuture.completedFuture(null);
        }

        ApiStep step = persona.steps.get(stepIndex);

        if (shouldStop()) {
            return CompletableFuture.completedFuture(null);
        }

        // Pipeline per step: TPS gate → budget → HTTP → metrics → think time → next step.
        return acquireSendGate()
                .thenCompose(ignored -> checkStopAndAcquireBudget())
                .thenCompose(granted -> executeStepIfGranted(granted, step, variableStore))
                .handle((metric, error) -> recordMetrics(step, metric, error))
                .thenCompose(ignored -> scheduleThinkTime(step))
                .thenCompose(ignored -> runAllSteps(stepIndex + 1, variableStore));
    }

    private CompletableFuture<Boolean> acquireSendGate() {
        if (throughputController != null) {
            return throughputController.acquireAsync().thenApply(ignored -> true);
        }
        return CompletableFuture.completedFuture(true);
    }

    private CompletableFuture<Boolean> checkStopAndAcquireBudget() {
        if (shouldStop()) {
            return CompletableFuture.completedFuture(false);
        }
        return acquireBudget();
    }

    private CompletableFuture<RequestMetric> executeStepIfGranted(
            Boolean granted,
            ApiStep step,
            VariableStore variableStore
    ) {
        if (!granted) {
            return CompletableFuture.completedFuture(null);
        }
        // Budget was acquired but the test window closed before send — refund the permit.
        if (shouldStop()) {
            releaseBudget();
            return CompletableFuture.completedFuture(null);
        }
        return stepExecutor.executeAsync(userId, persona, step, variableStore);
    }

    private Void recordMetrics(ApiStep step, RequestMetric metric, Throwable error) {
        if (error != null) {
            metricsCollector.add(
                    RequestMetric.failed(userId, persona.name, step.name, error.getMessage())
            );
        } else if (metric != null) {
            metricsCollector.add(metric);
        }
        return null;
    }

    private CompletableFuture<Boolean> acquireBudget() {
        if (requestModePacer != null) {
            return requestModePacer.acquire(persona.name);
        }
        if (requestLimiter != null) {
            return CompletableFuture.completedFuture(requestLimiter.tryAcquire(persona.name));
        }
        return CompletableFuture.completedFuture(true);
    }

    private void releaseBudget() {
        if (requestModePacer != null) {
            requestModePacer.release(persona.name);
        }
    }

    private boolean isBudgetExhausted() {
        if (requestModePacer != null) {
            return requestModePacer.isExhausted(persona.name);
        }
        if (requestLimiter != null) {
            return requestLimiter.isExhausted(persona.name);
        }
        return false;
    }

    private CompletableFuture<Void> scheduleThinkTime(ApiStep step) {
        if (step.thinkTimeMs == null || step.thinkTimeMs <= 0) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> delay = new CompletableFuture<>();
        SCHEDULER.schedule(
                () -> delay.complete(null),
                step.thinkTimeMs,
                TimeUnit.MILLISECONDS
        );
        return delay;
    }

    private boolean shouldStop() {
        if (keepAlive != null && !keepAlive.get()) {
            return true;
        }
        return System.currentTimeMillis() >= endTimeMillis;
    }
}

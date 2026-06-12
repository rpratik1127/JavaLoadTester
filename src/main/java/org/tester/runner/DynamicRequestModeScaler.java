package org.tester.runner;

import org.tester.control.RequestModePacer;
import org.tester.control.ThroughputController;
import org.tester.executor.HttpExecutor;
import org.tester.executor.StepExecutor;
import org.tester.metrics.MetricsCollector;
import org.tester.model.Persona;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dynamically spawns virtual users in total-request mode so each persona completes
 * its request budget within the configured duration.
 * <p>
 * Scaling is driven by observed RPS, linear time progress, permit slack from
 * {@link RequestModePacer}, and HTTP in-flight headroom from {@link HttpExecutor}.
 */
public class DynamicRequestModeScaler {

    /** Consumed count below this fraction of the linear goal triggers aggressive scaling. */
    private static final double PROGRESS_THRESHOLD = 0.95;
    /** Projected final count below this fraction of target triggers scaling. */
    private static final double TARGET_COMPLETION_RATIO = 0.98;
    private static final int SCALE_INTERVAL_FAST_MS = 250;
    private static final int SCALE_INTERVAL_SLOW_MS = 500;
    /** Do not spawn when fewer than this many HTTP in-flight slots remain. */
    private static final int MIN_IN_FLIGHT_HEADROOM = 500;

    private final ExecutorService executorService;
    private final long startTimeMillis;
    private final long endTimeMillis;
    private final int durationSeconds;
    private final int rampUpSeconds;
    private final MetricsCollector metricsCollector;
    private final StepExecutor stepExecutor;
    private final ThroughputController throughputController;
    private final RequestModePacer requestModePacer;
    private final Map<String, Integer> requestTargets;
    private final List<Persona> personas;

    private final Map<String, AtomicInteger> activeUsers = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> spawnedUsers = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> nextUserId = new ConcurrentHashMap<>();
    private final AtomicInteger totalSpawned = new AtomicInteger(0);

    private final ScheduledExecutorService scalerExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "request-mode-scaler");
                t.setDaemon(true);
                return t;
            });

    public DynamicRequestModeScaler(
            ExecutorService executorService,
            long startTimeMillis,
            long endTimeMillis,
            int durationSeconds,
            int rampUpSeconds,
            MetricsCollector metricsCollector,
            StepExecutor stepExecutor,
            ThroughputController throughputController,
            RequestModePacer requestModePacer,
            Map<String, Integer> requestTargets,
            List<Persona> personas
    ) {
        this.executorService = executorService;
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.durationSeconds = durationSeconds;
        this.rampUpSeconds = rampUpSeconds;
        this.metricsCollector = metricsCollector;
        this.stepExecutor = stepExecutor;
        this.throughputController = throughputController;
        this.requestModePacer = requestModePacer;
        this.requestTargets = requestTargets;
        this.personas = personas;
    }

    public void start() {
        for (Persona persona : personas) {
            int target = requestTargets.getOrDefault(persona.name, 0);
            if (target <= 0) {
                continue;
            }

            activeUsers.put(persona.name, new AtomicInteger(0));
            spawnedUsers.put(persona.name, new AtomicInteger(0));
            nextUserId.put(persona.name, new AtomicInteger(1));

            int required = maxConcurrentUsers(persona.name, target);
            int initial = rampUpSeconds <= 0 ? required : rampUserCap(0, required);
            for (int i = 0; i < initial; i++) {
                spawnUser(persona);
            }

            System.out.printf(
                    "Request mode: starting %d virtual user(s) for %s (target: %d requests in %d sec)%n",
                    initial, persona.name, target, durationSeconds
            );
        }

        scheduleNextScale(SCALE_INTERVAL_FAST_MS);
    }

    private void scheduleNextScale(long delayMs) {
        scalerExecutor.schedule(() -> {
            if (System.currentTimeMillis() >= endTimeMillis) {
                return;
            }

            boolean behind = scale();
            long nextDelay = behind ? SCALE_INTERVAL_FAST_MS : SCALE_INTERVAL_SLOW_MS;
            scheduleNextScale(nextDelay);
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private boolean scale() {
        long now = System.currentTimeMillis();
        if (now >= endTimeMillis) {
            return false;
        }

        ScalingTickContext tick = ScalingTickContext.of(now, startTimeMillis, endTimeMillis, durationSeconds);
        boolean anyBehind = false;

        for (Persona persona : personas) {
            if (scalePersona(persona, tick)) {
                anyBehind = true;
            }
        }

        return anyBehind;
    }

    /**
     * Evaluates one persona and spawns additional VUs when behind schedule,
     * projected to miss the target, or when budget permits are unused.
     *
     * @return {@code true} if this persona still needs more concurrency
     */
    private boolean scalePersona(Persona persona, ScalingTickContext tick) {
        int target = requestTargets.getOrDefault(persona.name, 0);
        if (target <= 0) {
            return false;
        }

        if (requestModePacer.isExhausted(persona.name)) {
            return false;
        }

        int consumed = requestModePacer.getConsumed(persona.name);
        if (consumed >= target) {
            return false;
        }

        int remaining = target - consumed;
        double observedRps = (double) consumed / tick.elapsedSec();
        double requiredRps = (double) remaining / tick.remainingSec();
        double projectedTotal = consumed + observedRps * tick.remainingSec();

        long goalToDate = (long) (target * tick.progress());
        boolean behindGoalToDate = goalToDate > 0 && consumed < goalToDate * PROGRESS_THRESHOLD;
        boolean willMissTarget = projectedTotal < target * TARGET_COMPLETION_RATIO;
        boolean permitsUnused = requestModePacer.getPermitSlack(persona.name) >= 2;

        int spawned = spawnedUsers.get(persona.name).get();
        int neededUsers = usersNeededForRate(persona.name, target, requiredRps);
        int maxUsers = Math.max(neededUsers, maxConcurrentUsers(persona.name, target));
        int userCap = behindGoalToDate || willMissTarget || permitsUnused
                ? maxUsers
                : rampUserCap(tick.elapsedMs() / 1000, maxUsers);

        if (!behindGoalToDate && !willMissTarget && !permitsUnused && spawned >= neededUsers) {
            return false;
        }

        // At cap but still behind — keep fast scaling ticks; no new spawns this round.
        if (spawned >= userCap) {
            return true;
        }

        int usersToAdd = computeUsersToAdd(
                persona,
                consumed,
                goalToDate,
                spawned,
                neededUsers,
                userCap,
                observedRps,
                behindGoalToDate,
                permitsUnused
        );

        spawnUsers(persona, usersToAdd, userCap);

        return true;
    }

    private int computeUsersToAdd(
            Persona persona,
            int consumed,
            long goalToDate,
            int spawned,
            int neededUsers,
            int userCap,
            double observedRps,
            boolean behindGoalToDate,
            boolean permitsUnused
    ) {
        int usersToAdd = Math.max(1, neededUsers - spawned);

        if (behindGoalToDate && goalToDate > consumed && spawned > 0 && observedRps > 0) {
            long deficit = goalToDate - consumed;
            double perUserRps = observedRps / spawned;
            int deficitUsers = (int) Math.ceil(deficit / Math.max(1.0, perUserRps));
            usersToAdd = Math.max(usersToAdd, deficitUsers);
        }

        if (permitsUnused && spawned > 0 && observedRps > 0) {
            long slack = requestModePacer.getPermitSlack(persona.name);
            double perUserRps = observedRps / spawned;
            int slackUsers = (int) Math.ceil(slack / Math.max(1.0, perUserRps));
            usersToAdd = Math.max(usersToAdd, slackUsers);
        }

        return Math.min(usersToAdd, userCap - spawned);
    }

    private void spawnUsers(Persona persona, int usersToAdd, int userCap) {
        for (int i = 0; i < usersToAdd; i++) {
            if (spawnedUsers.get(persona.name).get() >= userCap) {
                break;
            }
            if (!canSpawnMore()) {
                break;
            }
            spawnUser(persona);
        }
    }

    private int usersNeededForRate(String personaName, int targetRequests, double requiredRps) {
        double avgLatencySec = metricsCollector.getAverageResponseTimeForPersona(personaName) / 1000.0;
        if (avgLatencySec <= 0) {
            avgLatencySec = 0.1; // warm-up default before first responses arrive
        }
        int fromRate = (int) Math.ceil(requiredRps * avgLatencySec * 2.0);
        int fromTarget = RequestModePacer.computeRequiredUsers(
                targetRequests, durationSeconds, avgLatencySec
        );
        return Math.max(fromRate, fromTarget);
    }

    private int maxConcurrentUsers(String personaName, int targetRequests) {
        int targetRps = RequestModePacer.computeTargetRps(targetRequests, durationSeconds);
        double avgLatencySec = metricsCollector.getAverageResponseTimeForPersona(personaName) / 1000.0;
        int base = RequestModePacer.computeRequiredUsers(targetRequests, durationSeconds, avgLatencySec);
        return Math.max(base, Math.min(targetRps * 3, targetRequests));
    }

    private int rampUserCap(long elapsedSec, int maxConcurrentUsers) {
        if (rampUpSeconds <= 0 || elapsedSec >= rampUpSeconds) {
            return maxConcurrentUsers;
        }

        double progress = Math.min(1.0, (double) elapsedSec / rampUpSeconds);
        return Math.max(1, (int) Math.ceil(maxConcurrentUsers * progress));
    }

    private boolean canSpawnMore() {
        return HttpExecutor.getAvailableInFlightPermits() >= MIN_IN_FLIGHT_HEADROOM;
    }

    private void spawnUser(Persona persona) {
        if (!canSpawnMore()) {
            return;
        }

        AtomicInteger userIdCounter = nextUserId.get(persona.name);
        AtomicInteger active = activeUsers.get(persona.name);
        if (userIdCounter == null || active == null) {
            return;
        }

        int localUserId = userIdCounter.getAndIncrement();
        String userId = VirtualUserIds.format(persona.name, localUserId);

        active.incrementAndGet();
        spawnedUsers.get(persona.name).incrementAndGet();
        totalSpawned.incrementAndGet();

        VirtualUser virtualUser = VirtualUser.forRequestMode(
                userId,
                persona,
                endTimeMillis,
                metricsCollector,
                stepExecutor,
                throughputController,
                requestModePacer,
                active
        );

        try {
            executorService.submit(virtualUser);
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // Roll back counters when the executor is shutting down.
            active.decrementAndGet();
            spawnedUsers.get(persona.name).decrementAndGet();
            totalSpawned.decrementAndGet();
        }
    }

    public int getTotalSpawned() {
        return totalSpawned.get();
    }

    public Map<String, Integer> getActiveUsersSnapshot() {
        return AtomicCounterSnapshots.snapshot(activeUsers);
    }

    public Map<String, Integer> getSpawnedUsersSnapshot() {
        return AtomicCounterSnapshots.snapshot(spawnedUsers);
    }

    public Map<String, Integer> getPeakActiveUsersSnapshot() {
        // Peak tracking was removed; snapshot reflects current active count only.
        return getActiveUsersSnapshot();
    }

    public void shutdown() {
        scalerExecutor.shutdownNow();
    }

    /** Time-derived values reused for every persona on a single scaler tick. */
    private record ScalingTickContext(long elapsedMs, long elapsedSec, double progress, long remainingSec) {

        static ScalingTickContext of(
                long now,
                long startTimeMillis,
                long endTimeMillis,
                int durationSeconds
        ) {
            long elapsedMs = now - startTimeMillis;
            long elapsedSec = Math.max(1, elapsedMs / 1000);
            double progress = Math.min(1.0, elapsedMs / (durationSeconds * 1000.0));
            long remainingSec = Math.max(1, (endTimeMillis - now) / 1000);
            return new ScalingTickContext(elapsedMs, elapsedSec, progress, remainingSec);
        }
    }
}

package org.tester.runner;

import org.tester.config.TestConstants;
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
    private static final int MIN_IN_FLIGHT_HEADROOM = 100;
    private static final int MAX_SPAWN_PER_TICK = 100;
    private static final double WARMUP_LATENCY_SEC = 0.15;
    /**
     * Allow enough VUs for target RPS at ~1s RTT (Little's law) without over-spawning.
     */
    private static final int MAX_VUS_PER_TARGET_RPS = 4;
    /** Only treat latency as overload when it exceeds this AND schedule is on track. */
    private static final double MAX_LATENCY_FOR_SCALING_SEC = 15.0;

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
            int initial = computeInitialSpawn(persona.name, target, required);
            for (int i = 0; i < initial; i++) {
                spawnUser(persona);
            }

            int actualSpawned = spawnedUsers.get(persona.name).get();
            System.out.printf(
                    "Request mode: starting %d virtual user(s) for %s (target: %d requests in %d sec, est. %d VUs needed)%n",
                    actualSpawned, persona.name, target, durationSeconds, required
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

        int spawned = spawnedUsers.get(persona.name).get();
        double observedLatencySec = metricsCollector.getAverageResponseTimeForPersona(persona.name) / 1000.0;
        int userCap = computeUserCap(persona.name, target);

        // High latency with enough VUs — only stop scaling when not behind schedule.
        if (observedLatencySec > MAX_LATENCY_FOR_SCALING_SEC
                && spawned >= userCap / 2
                && !behindGoalToDate
                && !willMissTarget) {
            return false;
        }

        int neededUsers = usersNeededForRate(persona.name, target, requiredRps);

        if (!behindGoalToDate && !willMissTarget && spawned >= neededUsers) {
            return false;
        }

        // At cap but still behind — keep fast scaling ticks; no new spawns this round.
        if (spawned >= userCap) {
            return behindGoalToDate || willMissTarget;
        }

        int usersToAdd = computeUsersToAdd(
                persona,
                consumed,
                goalToDate,
                spawned,
                neededUsers,
                userCap,
                observedRps,
                behindGoalToDate
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
            boolean behindGoalToDate
    ) {
        int usersToAdd = Math.max(1, neededUsers - spawned);

        if (behindGoalToDate && goalToDate > consumed && spawned > 0 && observedRps > 0) {
            long deficit = goalToDate - consumed;
            double perUserRps = observedRps / spawned;
            int deficitUsers = (int) Math.ceil(deficit / Math.max(1.0, perUserRps));
            usersToAdd = Math.max(usersToAdd, Math.min(deficitUsers, MAX_SPAWN_PER_TICK));
        }

        return Math.min(usersToAdd, userCap - spawned);
    }

    private int computeInitialSpawn(String personaName, int target, int required) {
        int targetRps = RequestModePacer.computeTargetRps(target, durationSeconds);
        // Start with enough VUs for target RPS at ~1s latency, not a tiny warm-up fraction.
        int startAtOneSec = RequestModePacer.computeRequiredUsers(target, durationSeconds, 1.0);
        int warmStart = RequestModePacer.computeRequiredUsers(
                target, durationSeconds, WARMUP_LATENCY_SEC
        );
        int initial = Math.min(required, Math.max(startAtOneSec, warmStart));
        if (rampUpSeconds <= 0) {
            int floor = TestConstants.estimateUsersForRps(
                    Math.max(targetRps, TestConstants.TARGET_TPS),
                    1.0
            );
            return Math.min(initial, Math.max(floor, targetRps));
        }
        return Math.max(1, Math.min(required, rampUserCap(0, initial)));
    }

    private void spawnUsers(Persona persona, int usersToAdd, int userCap) {
        int batch = Math.min(usersToAdd, MAX_SPAWN_PER_TICK);
        for (int i = 0; i < batch; i++) {
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
        double avgLatencySec = cappedLatencySec(personaName);
        int fromRate = (int) Math.ceil(requiredRps * avgLatencySec * 2.0);
        int fromTarget = RequestModePacer.computeRequiredUsers(
                targetRequests, durationSeconds, avgLatencySec
        );
        return Math.min(computeUserCap(personaName, targetRequests), Math.max(fromRate, fromTarget));
    }

    private int maxConcurrentUsers(String personaName, int targetRequests) {
        return computeUserCap(personaName, targetRequests);
    }

    private int computeUserCap(String personaName, int targetRequests) {
        int targetRps = RequestModePacer.computeTargetRps(targetRequests, durationSeconds);
        double latencySec = cappedLatencySec(personaName);
        int fromLittle = RequestModePacer.computeRequiredUsers(
                targetRequests, durationSeconds, latencySec
        );
        int floorForTarget = TestConstants.estimateUsersForRps(
                Math.max(targetRps, TestConstants.TARGET_TPS),
                1.0
        );
        int absoluteMax = Math.max(
                targetRps * MAX_VUS_PER_TARGET_RPS,
                TestConstants.estimateUsersForRps(TestConstants.TARGET_TPS, latencySec)
        );
        return Math.min(targetRequests, Math.min(absoluteMax, Math.max(fromLittle, floorForTarget)));
    }

    private double cappedLatencySec(String personaName) {
        double observed = metricsCollector.getAverageResponseTimeForPersona(personaName) / 1000.0;
        if (observed <= 0) {
            return WARMUP_LATENCY_SEC;
        }
        return Math.min(observed, MAX_LATENCY_FOR_SCALING_SEC);
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
                null,
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

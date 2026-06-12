package org.tester.control;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-persona request budget for total-request mode.
 * Each persona gets exactly {@code requestTargets} requests over the test duration.
 */
public class RequestModePacer {

    private final int durationSeconds;
    private final Map<String, PersonaRequestBudget> budgets = new ConcurrentHashMap<>();

    public RequestModePacer(
            Map<String, Integer> requestTargets,
            long startTimeMillis,
            int durationSeconds
    ) {
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("durationSeconds must be positive");
        }

        this.durationSeconds = durationSeconds;
        long durationMillis = durationSeconds * 1000L;

        for (Map.Entry<String, Integer> entry : requestTargets.entrySet()) {
            int target = entry.getValue();
            if (target <= 0) {
                continue;
            }

            int targetRps = computeTargetRps(target, durationSeconds);
            budgets.put(
                    entry.getKey(),
                    new PersonaRequestBudget(target, startTimeMillis, durationMillis)
            );

            System.out.printf(
                    "[RequestModePacer] %s: exactly %d requests over %d sec -> %d req/s%n",
                    entry.getKey(), target, durationSeconds, targetRps
            );
        }
    }

    public static int computeTargetRps(int requestCount, int durationSeconds) {
        if (requestCount <= 0 || durationSeconds <= 0) {
            return 0;
        }
        return (int) Math.ceil((double) requestCount / durationSeconds);
    }

    public static int computeRequiredUsers(int requestCount, int durationSeconds, double avgLatencySec) {
        int targetRps = computeTargetRps(requestCount, durationSeconds);
        if (avgLatencySec <= 0) {
            avgLatencySec = 0.15;
        }
        // Little's law style estimate with safety factor for slow responses.
        return Math.max(1, (int) Math.ceil(targetRps * avgLatencySec * 2.5));
    }

    public CompletableFuture<Boolean> acquire(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        if (budget == null) {
            return CompletableFuture.completedFuture(false);
        }
        return budget.acquire();
    }

    public void release(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        if (budget != null) {
            budget.releaseOne();
        }
    }

    public boolean isExhausted(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        return budget == null || budget.isExhausted();
    }

    public int getTargetRps(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        if (budget == null) {
            return 0;
        }
        return computeTargetRps(budget.getLimit(), durationSeconds);
    }

    public int getConsumed(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        return budget == null ? 0 : budget.getConsumed();
    }

    public int getLimit(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        return budget == null ? 0 : budget.getLimit();
    }

    public int getRemaining(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        if (budget == null) {
            return 0;
        }
        return Math.max(0, budget.getLimit() - budget.getConsumed());
    }

    public long getPermittedByNow(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        return budget == null ? 0 : budget.getPermittedByNow();
    }

    /**
     * True when the budget has released permits that have not yet been consumed.
     * More VUs only help when this is true; otherwise the budget is the bottleneck.
     */
    public boolean hasAvailablePermits(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        if (budget == null || budget.isExhausted() || budget.isExpired()) {
            return false;
        }
        return budget.getPermittedByNow() > budget.getConsumed();
    }

    public boolean isExpired(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        return budget != null && budget.isExpired();
    }

    /** Permits released by the schedule but not yet consumed — signals need for more concurrency. */
    public long getPermitSlack(String personaName) {
        PersonaRequestBudget budget = budgets.get(personaName);
        if (budget == null || budget.isExhausted() || budget.isExpired()) {
            return 0;
        }
        return budget.getPermittedByNow() - budget.getConsumed();
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public void shutdown() {
        budgets.values().forEach(PersonaRequestBudget::shutdown);
        budgets.clear();
        BudgetScheduler.shutdown();
    }
}

package org.tester.control;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Shared scheduler for all {@link PersonaRequestBudget} instances — one thread instead of one per persona.
 */
public final class BudgetScheduler {

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "request-budget-scheduler");
                t.setDaemon(true);
                return t;
            });

    private BudgetScheduler() {
    }

    public static void register(PersonaRequestBudget budget) {
        SCHEDULER.scheduleAtFixedRate(budget::tick, 0, PersonaRequestBudget.TICK_MS, TimeUnit.MILLISECONDS);
    }

    public static void shutdown() {
        SCHEDULER.shutdownNow();
    }
}

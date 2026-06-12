package org.tester.control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple per-persona request counter used outside total-request mode pacing.
 */
public class PersonaRequestLimiter {

    private final Map<String, Integer> limits;
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    public PersonaRequestLimiter(Map<String, Integer> limits) {
        this.limits = limits;
        for (String personaName : limits.keySet()) {
            counts.put(personaName, new AtomicInteger(0));
        }
    }

    public boolean isExhausted(String personaName) {
        Integer limit = limits.get(personaName);
        if (limit == null || limit <= 0) {
            return true;
        }

        AtomicInteger count = counts.get(personaName);
        return count != null && count.get() >= limit;
    }

    public boolean tryAcquire(String personaName) {
        Integer limit = limits.get(personaName);
        if (limit == null || limit <= 0) {
            return false;
        }

        AtomicInteger count = counts.get(personaName);
        while (true) {
            int current = count.get();
            if (current >= limit) {
                return false;
            }
            if (count.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }
}

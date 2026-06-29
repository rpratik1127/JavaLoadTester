package org.tester.runner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Converts live {@link AtomicInteger} counters into immutable snapshots for reporting.
 */
final class AtomicCounterSnapshots {

    private AtomicCounterSnapshots() {
    }

    static Map<String, Integer> snapshot(Map<String, AtomicInteger> counters) {
        Map<String, Integer> result = new ConcurrentHashMap<>();
        counters.forEach((name, count) -> result.put(name, count.get()));
        return result;
    }
}

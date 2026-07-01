package org.tester.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Per-virtual-user variables extracted from prior step responses. */
public class VariableStore {

    private final Map<String, String> variables = new ConcurrentHashMap<>();

    /** Stores an extracted variable for use in later steps. */
    public void put(String key, String value) {
        variables.put(key, value);
    }

    /** Returns a previously stored variable, or null when absent. */
    public String get(String key) {
        return variables.get(key);
    }

    public boolean contains(String key) {
        return variables.containsKey(key);
    }
}
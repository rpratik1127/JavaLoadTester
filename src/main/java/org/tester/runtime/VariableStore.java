package org.tester.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Per-virtual-user variables extracted from prior step responses. */
public class VariableStore {

    private final Map<String, String> variables = new ConcurrentHashMap<>();

    public void put(String key, String value) {
        variables.put(key, value);
    }

    public String get(String key) {
        return variables.get(key);
    }

    public boolean contains(String key) {
        return variables.containsKey(key);
    }
}
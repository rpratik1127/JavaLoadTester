package org.tester.selector;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable load configuration produced by {@link PersonaUserSelector}.
 * <p>
 * {@link #getTotalUsers()} and {@link #getSpawnedPerPersona()} are populated after a run
 * completes and are read by reporting ({@link org.tester.report.CsvReportGenerator}).
 */
public class PersonaLoadConfig {

    /** Populated after user-mode selection or request-mode scaler shutdown. */
    private static int totalUsers = 0;
    /** Populated after request-mode scaler shutdown. */
    private static Map<String, Integer> spawnedPerPersona = Map.of();

    public final LoadInputMode mode;
    public final Map<String, Integer> valuesPerPersona;

    public PersonaLoadConfig(LoadInputMode mode, Map<String, Integer> valuesPerPersona) {
        this.mode = mode;
        this.valuesPerPersona = Collections.unmodifiableMap(new LinkedHashMap<>(valuesPerPersona));
    }

    public static void setTotalUsers(int count) {
        totalUsers = count;
    }

    public static int getTotalUsers() {
        return totalUsers;
    }

    public static void setSpawnedPerPersona(Map<String, Integer> spawned) {
        spawnedPerPersona = spawned == null
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(spawned));
    }

    public static Map<String, Integer> getSpawnedPerPersona() {
        return spawnedPerPersona;
    }
}

package org.tester.report;

import org.tester.metrics.MetricsCollector;
import org.tester.model.Persona;
import org.tester.selector.PersonaLoadConfig;

import java.util.List;
import java.util.Map;

/**
 * Prints request-mode completion lines using the same metrics snapshot as CSV reporting.
 */
public final class RequestModeCompletionReporter {

    private RequestModeCompletionReporter() {
    }

    public static void print(
            MetricsCollector metricsCollector,
            PersonaLoadConfig loadConfig,
            List<Persona> personas
    ) {
        Map<String, Integer> spawnedSnapshot = PersonaLoadConfig.getSpawnedPerPersona();

        for (Persona persona : personas) {
            int target = loadConfig.valuesPerPersona.getOrDefault(persona.name, 0);
            if (target <= 0) {
                continue;
            }

            long recorded = metricsCollector.getTotalRequestsForPersona(persona.name);
            System.out.printf(
                    "Request mode: %s finished with %d virtual users spawned, %d/%d requests recorded%n",
                    persona.name,
                    spawnedSnapshot.getOrDefault(persona.name, 0),
                    recorded,
                    target
            );
        }
    }
}

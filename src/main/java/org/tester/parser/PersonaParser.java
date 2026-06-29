package org.tester.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tester.model.ApiStep;
import org.tester.model.Persona;
import org.tester.model.TestPlan;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads persona definitions from JSON and pre-serializes static request bodies
 * (bodies without {@code ${variables}}) to avoid Jackson work on the hot path.
 */
public class PersonaParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public TestPlan parse(String filePath) throws Exception {
        Path path = Path.of(filePath);

        if (!Files.exists(path)) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        try (InputStream inputStream = Files.newInputStream(path)) {
            TestPlan plan = objectMapper.readValue(inputStream, TestPlan.class);
            precomputeStaticArtifacts(plan);
            return plan;
        }
    }

    private void precomputeStaticArtifacts(TestPlan plan) throws Exception {
        if (plan.personas == null) {
            return;
        }

        for (Persona persona : plan.personas) {
            if (persona.steps == null) {
                continue;
            }

            for (ApiStep step : persona.steps) {
                if (step.body != null && !step.body.isEmpty()) {
                    String json = objectMapper.writeValueAsString(step.body);
                    if (!json.contains("${")) {
                        step.cachedJsonBody = json;
                        step.cachedBodyBytes = json.getBytes(StandardCharsets.UTF_8);
                    }
                }
                StaticHttpRequestCache.precompute(persona, step);
            }
        }
    }
}

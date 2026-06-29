package org.tester.executor;

import org.tester.metrics.MetricsCollector;
import org.tester.metrics.RequestMetric;
import org.tester.model.ApiStep;
import org.tester.model.Persona;
import org.tester.model.ProtocolType;
import org.tester.runtime.VariableStore;

import java.util.concurrent.CompletableFuture;

/** Dispatches a single persona step to the HTTP executor. */
public class StepExecutor {

    private final HttpExecutor httpExecutor;

    public StepExecutor(MetricsCollector metricsCollector) {
        this.httpExecutor = new HttpExecutor(metricsCollector);
    }

    public CompletableFuture<RequestMetric> executeAsync(
            String userId,
            Persona persona,
            ApiStep step,
            VariableStore variableStore
    ) {
        if (isHttpProtocol(step.protocol)) {
            return httpExecutor.executeAsync(
                    userId,
                    persona.name,
                    persona.baseUrl,
                    step,
                    variableStore
            );
        }

        return CompletableFuture.failedFuture(
                new IllegalArgumentException("Unsupported protocol: " + step.protocol)
        );
    }

    private static boolean isHttpProtocol(ProtocolType protocol) {
        return protocol == ProtocolType.HTTP
                || protocol == ProtocolType.HTTPS
                || protocol == ProtocolType.REST;
    }
}

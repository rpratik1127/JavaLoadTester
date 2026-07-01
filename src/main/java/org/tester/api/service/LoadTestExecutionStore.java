package org.tester.api.service;

import org.springframework.stereotype.Component;
import org.tester.api.dto.LoadTestResponse;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory store of load-test execution snapshots keyed by execution id. */
@Component
public class LoadTestExecutionStore {

    private final ConcurrentHashMap<String, LoadTestResponse> executions = new ConcurrentHashMap<>();

    /** Upserts the latest status and metrics for an execution. */
    public void save(String executionId, LoadTestResponse response) {
        executions.put(executionId, response);
    }

    /** Looks up a stored execution snapshot by id. */
    public Optional<LoadTestResponse> findById(String executionId) {
        return Optional.ofNullable(executions.get(executionId));
    }
}

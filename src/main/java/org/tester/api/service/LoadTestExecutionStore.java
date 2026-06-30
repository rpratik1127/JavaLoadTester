package org.tester.api.service;

import org.springframework.stereotype.Component;
import org.tester.api.dto.LoadTestResponse;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LoadTestExecutionStore {

    private final ConcurrentHashMap<String, LoadTestResponse> executions = new ConcurrentHashMap<>();

    public void save(String executionId, LoadTestResponse response) {
        executions.put(executionId, response);
    }

    public Optional<LoadTestResponse> findById(String executionId) {
        return Optional.ofNullable(executions.get(executionId));
    }
}

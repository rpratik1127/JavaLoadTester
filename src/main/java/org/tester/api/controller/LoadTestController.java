package org.tester.api.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.tester.api.dto.LoadTestRequest;
import org.tester.api.dto.LoadTestResponse;
import org.tester.api.service.LoadTestExecutionStore;
import org.tester.api.service.LoadTestService;

@RestController
@RequestMapping("/api/load-test")
public class LoadTestController {

    private final LoadTestService loadTestService;
    private final LoadTestExecutionStore executionStore;

    public LoadTestController(LoadTestService loadTestService, LoadTestExecutionStore executionStore) {
        this.loadTestService = loadTestService;
        this.executionStore = executionStore;
    }

    @PostMapping("/run")
    public LoadTestResponse run(@RequestBody LoadTestRequest request) throws Exception {
        return loadTestService.runTest(request);
    }

    @GetMapping("/{executionId}")
    public LoadTestResponse getExecution(@PathVariable String executionId) {
        return executionStore.findById(executionId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Execution not found: " + executionId
                ));
    }
}

package org.tester.api.cli;

import org.springframework.stereotype.Component;
import org.tester.api.dto.LoadTestRequest;
import org.tester.api.service.LoadTestService;
import org.tester.config.CliOptions;
import org.tester.config.TestConstants;
import org.tester.model.TestPlan;
import org.tester.parser.PersonaParser;
import org.tester.selector.PersonaLoadConfig;
import org.tester.selector.PersonaUserSelector;
import org.tester.selector.RampUpSelector;
import org.tester.selector.TargetTpsSelector;
import org.tester.selector.TestDurationSelector;

/**
 * Builds a {@link LoadTestRequest} from interactive CLI selectors and delegates to {@link LoadTestService}.
 */
@Component
public class LoadTestCliRunner {

    private final LoadTestService loadTestService;
    private final PersonaParser personaParser;

    public LoadTestCliRunner(LoadTestService loadTestService, PersonaParser personaParser) {
        this.loadTestService = loadTestService;
        this.personaParser = personaParser;
    }

    public void run(String[] args) throws Exception {
        CliOptions options = CliOptions.fromArgs(args);

        TestPlan testPlan = personaParser.parse(TestConstants.DEFAULT_PERSONA_FILE);
        PersonaLoadConfig loadConfig = new PersonaUserSelector().selectLoadConfig(testPlan.personas);

        LoadTestRequest request = new LoadTestRequest();
        request.setPersona(testPlan);
        request.setLoadInputMode(loadConfig.mode);
        request.setValuesPerPersona(loadConfig.valuesPerPersona);
        request.setDurationSeconds(new TestDurationSelector().selectDurationInSeconds());
        request.setRampUpSeconds(new RampUpSelector().selectRampUpSeconds(request.getDurationSeconds()));
        request.setTargetTps(new TargetTpsSelector().selectTargetTps());
        request.setGenerateRequestLog(options.generateRequestLog);
        request.setTrackSentRps(options.trackSentRps);
        request.setConnectionMode(options.connectionMode);
        request.setLiveReportingEnabled(true);

        loadTestService.runTest(request);
    }
}

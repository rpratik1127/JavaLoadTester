package org.tester.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.tester.parser.PersonaParser;
import org.tester.report.CsvReportGenerator;
import org.tester.report.ReportGenerator;
import org.tester.runner.ConcurrentPersonaRunner;

/** Spring beans wiring the core load-test engine components. */
@Configuration
public class LoadTestEngineConfig {

    /** JSON persona loader with static-body precomputation. */
    @Bean
    PersonaParser personaParser() {
        return new PersonaParser();
    }

    /** Terminal and CSV summary reporter. */
    @Bean
    ReportGenerator reportGenerator() {
        return new ReportGenerator();
    }

    /** Step-level and per-request CSV writer. */
    @Bean
    CsvReportGenerator csvReportGenerator() {
        return new CsvReportGenerator();
    }

    /** Orchestrates persona execution in user or request mode. */
    @Bean
    ConcurrentPersonaRunner concurrentPersonaRunner() {
        return new ConcurrentPersonaRunner();
    }
}

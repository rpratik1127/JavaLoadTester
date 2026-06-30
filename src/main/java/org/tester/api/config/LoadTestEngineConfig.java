package org.tester.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.tester.parser.PersonaParser;
import org.tester.report.CsvReportGenerator;
import org.tester.report.ReportGenerator;
import org.tester.runner.ConcurrentPersonaRunner;

@Configuration
public class LoadTestEngineConfig {

    @Bean
    PersonaParser personaParser() {
        return new PersonaParser();
    }

    @Bean
    ReportGenerator reportGenerator() {
        return new ReportGenerator();
    }

    @Bean
    CsvReportGenerator csvReportGenerator() {
        return new CsvReportGenerator();
    }

    @Bean
    ConcurrentPersonaRunner concurrentPersonaRunner() {
        return new ConcurrentPersonaRunner();
    }
}

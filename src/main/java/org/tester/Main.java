package org.tester;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.tester.api.cli.LoadTestCliRunner;

/**
 * CLI entry point for the Netty-based load tester.
 * <p>
 * Bootstraps a headless Spring context and delegates orchestration to {@link LoadTestCliRunner}.
 */
public class Main {

    /** Entry point for CLI-based execution via a headless Spring context. */
    public static void main(String[] args) throws Exception {
        try (var context = new SpringApplicationBuilder(LoadTesterApplication.class)
                .web(WebApplicationType.NONE)
                .run(args)) {
            context.getBean(LoadTestCliRunner.class).run(args);
        }
    }
}

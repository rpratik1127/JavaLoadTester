package org.tester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the HTTP load-test engine (REST API mode).
 */
@SpringBootApplication
public class LoadTesterApplication {

    /** Starts the web application when launched as a Spring Boot jar. */
    public static void main(String[] args) {
        SpringApplication.run(LoadTesterApplication.class, args);
    }
}

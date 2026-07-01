package org.tester.executor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rate-limited logging for HTTP hot paths. Avoids stderr flooding under load.
 */
final class HttpDebugLog {

    private static final int MAX_WARNINGS = 32;
    private static final AtomicInteger WARNING_COUNT = new AtomicInteger(0);

    private HttpDebugLog() {
    }

    /** Emits a rate-limited warning to stderr under load. */
    static void warn(String message) {
        if (WARNING_COUNT.incrementAndGet() <= MAX_WARNINGS) {
            System.err.println(message);
            if (WARNING_COUNT.get() == MAX_WARNINGS) {
                System.err.println("[HTTP] Further warnings suppressed.");
            }
        }
    }

    /** Emits a rate-limited warning with optional cause to stderr under load. */
    static void warn(String message, Throwable cause) {
        if (WARNING_COUNT.incrementAndGet() <= MAX_WARNINGS) {
            System.err.println(message + (cause == null ? "" : ": " + cause));
            if (WARNING_COUNT.get() == MAX_WARNINGS) {
                System.err.println("[HTTP] Further warnings suppressed.");
            }
        }
    }

    /** Prints a debug line when HTTP debug logging is enabled. */
    static void debug(String message) {
        if (HttpExecutor.isDebugEnabled()) {
            System.out.println(message);
        }
    }
}

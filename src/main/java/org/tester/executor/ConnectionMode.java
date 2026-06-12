package org.tester.executor;

/**
 * Controls how HTTP connections are acquired and reused.
 * <ul>
 *   <li>{@link #POOLED} — acquire from the host pool per request, release after response (k6/Gatling default).</li>
 *   <li>{@link #STICKY} — one connection per virtual user per host, serialized through a lease queue.</li>
 * </ul>
 */
public enum ConnectionMode {
    POOLED,
    STICKY
}

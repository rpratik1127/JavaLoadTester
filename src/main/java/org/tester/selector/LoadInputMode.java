package org.tester.selector;

/** How per-persona load values are interpreted at runtime. */
public enum LoadInputMode {
    /** Fixed number of concurrent virtual users per persona. */
    USERS,
    /** Fixed total HTTP attempts per persona over the test duration. */
    REQUESTS
}

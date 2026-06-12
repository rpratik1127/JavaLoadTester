package org.tester.model;

import java.util.Map;

/** One HTTP call within a persona flow. */
public class ApiStep {
    public String name;
    public ProtocolType protocol;
    public String method;
    public String path;
    public String url;

    public Map<String, String> queryParams;
    public Map<String, String> headers;
    public Map<String, Object> body;

    /** Pre-serialized JSON when body has no ${variables}; set at parse time. */
    public transient String cachedJsonBody;

    public Integer expectedStatus;
    public Integer thinkTimeMs;

    public Map<String, String> extract;
}

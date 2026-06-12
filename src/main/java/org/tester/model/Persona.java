package org.tester.model;

import java.util.List;

/** A user journey: base URL plus ordered API steps executed in a loop. */
public class Persona {
    public String baseUrl;
    public String name;
    public List<ApiStep> steps;
}
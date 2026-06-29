package org.tester.runtime;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Replaces {@code ${name}} placeholders using values from {@link VariableStore}. */
public class VariableResolver {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    public String resolve(String input, VariableStore variableStore) {
        if (input == null) {
            return null;
        }

        Matcher matcher = VARIABLE_PATTERN.matcher(input);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String variableName = matcher.group(1);
            String variableValue = variableStore.get(variableName);

            if (variableValue == null) {
                variableValue = "";
            }

            matcher.appendReplacement(result, Matcher.quoteReplacement(variableValue));
        }

        matcher.appendTail(result);

        return result.toString();
    }
}
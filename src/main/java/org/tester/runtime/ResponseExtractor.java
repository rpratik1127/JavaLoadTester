package org.tester.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Extracts values from JSON or plain-text response bodies using dot/bracket paths. */
public class ResponseExtractor {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public String extract(String responseBody, String path) throws Exception {
        if (responseBody == null || responseBody.isBlank()) return null;
        if (path == null || path.isBlank()) return null;

        // Handles plain text response body, like JWT token directly in response
        if ("$body".equals(path) || "body".equals(path) || "$".equals(path)) {
            return responseBody.trim();
        }

        // Handles JSON response body
        JsonNode currentNode = objectMapper.readTree(responseBody);
        String[] parts = path.split("[.\\[\\]]+");

        for (String part : parts) {
            if (part.isBlank() || currentNode == null) continue;

            if ("$".equals(part)) {
                continue;
            }

            if (part.matches("\\d+")) {
                currentNode = currentNode.get(Integer.parseInt(part));
            } else {
                if (!currentNode.has(part)) return null;
                currentNode = currentNode.get(part);
            }
        }

        if (currentNode == null || currentNode.isNull()) return null;
        return currentNode.isTextual() ? currentNode.asText() : currentNode.toString();
    }
}
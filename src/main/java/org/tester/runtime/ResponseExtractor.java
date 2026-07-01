package org.tester.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Extracts values from JSON or plain-text response bodies using dot/bracket paths. */
public class ResponseExtractor {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Resolves a JSON or plain-text path expression against the response body. */
    public String extract(String responseBody, String path) throws Exception {
        if (responseBody == null || responseBody.isBlank()) return null;
        if (path == null || path.isBlank()) return null;

        // Plain-text body shortcut: return the entire response when path is "$body" or "body".
        if ("$body".equals(path) || "body".equals(path) || "$".equals(path)) {
            return responseBody.trim();
        }

        // JSON path traversal for nested fields and array indices.
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
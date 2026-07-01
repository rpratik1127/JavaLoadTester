package org.tester.parser;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.tester.model.ApiStep;
import org.tester.model.Persona;

import java.net.URI;
import java.util.Map;

/** Builds immutable Netty request templates for static persona steps at parse time. */
final class StaticHttpRequestCache {

    private StaticHttpRequestCache() {
    }

    /** Builds and stores a Netty request template when the step has no dynamic variables. */
    static void precompute(Persona persona, ApiStep step) {
        if (!isFullyStatic(persona, step)) {
            return;
        }

        URI uri = URI.create(resolveUrl(persona, step));
        String methodName = step.method == null ? "GET" : step.method.toUpperCase();
        HttpMethod method = HttpMethod.valueOf(methodName);
        byte[] bodyBytes = step.cachedBodyBytes == null ? new byte[0] : step.cachedBodyBytes;
        String requestPath = requestPath(uri);

        FullHttpRequest template = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                method,
                requestPath,
                Unpooled.wrappedBuffer(bodyBytes)
        );

        HttpHeaders headers = template.headers();
        headers.set(HttpHeaderNames.HOST, hostHeader(uri));
        headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        headers.set(HttpHeaderNames.ACCEPT, "*/*");
        headers.set(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);

        if (bodyBytes.length > 0) {
            addDefaultContentType(headers, step);
        }

        if (step.headers != null) {
            for (Map.Entry<String, String> entry : step.headers.entrySet()) {
                headers.set(entry.getKey(), entry.getValue());
            }
        }

        step.cachedRequestTemplate = template;
    }

    /** True when URL, headers, body, and query params contain no {@code ${}} placeholders. */
    private static boolean isFullyStatic(Persona persona, ApiStep step) {
        if (step.url != null && step.url.contains("${")) {
            return false;
        }
        if (step.path != null && step.path.contains("${")) {
            return false;
        }
        if (step.queryParams != null && !step.queryParams.isEmpty()) {
            return false;
        }
        if (step.headers != null) {
            for (String value : step.headers.values()) {
                if (value != null && value.contains("${")) {
                    return false;
                }
            }
        }
        if (step.body != null && !step.body.isEmpty() && step.cachedBodyBytes == null) {
            return false;
        }
        return true;
    }

    private static String resolveUrl(Persona persona, ApiStep step) {
        if (step.url != null && !step.url.isBlank()) {
            return step.url;
        }
        return persona.baseUrl + step.path;
    }

    private static String requestPath(URI uri) {
        String path = uri.getRawPath();
        if (path == null || path.isBlank()) {
            path = "/";
        }
        String query = uri.getRawQuery();
        if (query != null && !query.isBlank()) {
            return path + "?" + query;
        }
        return path;
    }

    private static String hostHeader(URI uri) {
        int port = uri.getPort();
        boolean isHttps = "https".equalsIgnoreCase(uri.getScheme());
        int defaultPort = isHttps ? 443 : 80;
        if (port == -1 || port == defaultPort) {
            return uri.getHost();
        }
        return uri.getHost() + ":" + port;
    }

    private static void addDefaultContentType(HttpHeaders headers, ApiStep step) {
        if (step.headers != null) {
            for (String key : step.headers.keySet()) {
                if ("Content-Type".equalsIgnoreCase(key)) {
                    return;
                }
            }
        }
        headers.set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    }
}

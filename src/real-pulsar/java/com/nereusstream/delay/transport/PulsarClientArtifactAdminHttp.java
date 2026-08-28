package com.nereusstream.delay.transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Bounded, local-only HTTP access to the real P1 Pulsar admin surface. */
final class PulsarClientArtifactAdminHttp {
    private static final int MAX_REDIRECTS = 8;

    private PulsarClientArtifactAdminHttp() {}

    static HttpResponse<String> request(
            final HttpClient client, final String path, final String method, final String body) throws Exception {
        return request(client, path, method, body, null);
    }

    static HttpResponse<String> request(
            final HttpClient client,
            final String path,
            final String method,
            final String body,
            final Duration timeout)
            throws Exception {
        URI next = URI.create(path);
        validateInitial(next);
        final Set<Integer> allowedAdminPorts = allowedAdminPorts(next);
        final Set<URI> visited = new HashSet<>();
        for (int redirect = 0; redirect <= MAX_REDIRECTS; redirect++) {
            if (!visited.add(next)) {
                throw new IllegalStateException("Pulsar admin redirect loop: " + next);
            }
            final HttpRequest.Builder builder = HttpRequest.newBuilder(next)
                    .header("Content-Type", "application/json");
            if (timeout != null) {
                builder.timeout(timeout);
            }
            final HttpRequest request = buildRequest(builder, method, body);
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 307 && response.statusCode() != 308) {
                return response;
            }
            if (redirect == MAX_REDIRECTS) {
                throw new IllegalStateException(
                        "Pulsar admin redirect limit exceeded: " + next + " status=" + response.statusCode());
            }
            final String location = response.headers().firstValue("Location").orElse(null);
            if (location == null || location.isBlank()) {
                throw new IllegalStateException(
                        "Pulsar admin redirect had no Location: " + next + " status=" + response.statusCode());
            }
            final URI redirected = URI.create(location);
            validateRedirect(next, redirected, allowedAdminPorts);
            System.out.println("Pulsar admin owner redirect followed: " + next + " -> " + redirected);
            next = redirected;
        }
        throw new IllegalStateException("Pulsar admin redirect handling did not converge: " + path);
    }

    private static HttpRequest buildRequest(
            final HttpRequest.Builder builder, final String method, final String body) {
        final String requestBody = body == null ? "" : body;
        return switch (method) {
            case "DELETE" -> builder.DELETE().build();
            case "GET" -> builder.GET().build();
            case "POST" -> builder.POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                    .build();
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                    .build();
            default -> throw new IllegalArgumentException("unsupported HTTP method: " + method);
        };
    }

    private static void validateInitial(final URI initial) {
        if (!"http".equalsIgnoreCase(initial.getScheme())
                || !"127.0.0.1".equals(initial.getHost())
                || initial.getPort() <= 0
                || initial.getUserInfo() != null
                || initial.getFragment() != null
                || !isAdminPath(initial)) {
            throw new IllegalStateException("Pulsar admin request escaped the local admin scope: " + initial);
        }
    }

    private static Set<Integer> allowedAdminPorts(final URI initial) {
        final Set<Integer> ports = new HashSet<>();
        ports.add(initial.getPort());
        addConfiguredAdminPort(ports, "PULSAR_WEB_1_PORT");
        addConfiguredAdminPort(ports, "PULSAR_WEB_2_PORT");
        return ports;
    }

    private static void addConfiguredAdminPort(final Set<Integer> ports, final String variable) {
        final String value = System.getenv(variable);
        if (value == null || value.isBlank()) {
            return;
        }
        final int port;
        try {
            port = Integer.parseInt(value);
        } catch (NumberFormatException failure) {
            throw new IllegalStateException(variable + " must be a numeric TCP port: " + value, failure);
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalStateException(variable + " is outside the TCP port range: " + value);
        }
        ports.add(port);
    }

    private static void validateRedirect(
            final URI current, final URI redirected, final Set<Integer> allowedAdminPorts) {
        if (!"http".equalsIgnoreCase(current.getScheme())
                || !"http".equalsIgnoreCase(redirected.getScheme())
                || !"127.0.0.1".equals(current.getHost())
                || !"127.0.0.1".equals(redirected.getHost())
                || !allowedAdminPorts.contains(redirected.getPort())
                || !isAdminPath(redirected)
                || !Objects.equals(redirected.getRawPath(), current.getRawPath())
                || !allowedRedirectQuery(current, redirected)
                || redirected.getUserInfo() != null
                || redirected.getFragment() != null) {
            throw new IllegalStateException("Pulsar admin redirect escaped the configured local resource scope: "
                    + current + " -> " + redirected);
        }
    }

    /**
     * The broker may add its boolean owner-routing marker to a query-less
     * resource redirect. No other query mutation is within the bounded local
     * scope.
     */
    private static boolean allowedRedirectQuery(final URI current, final URI redirected) {
        if (Objects.equals(redirected.getRawQuery(), current.getRawQuery())) {
            return true;
        }
        return current.getRawQuery() == null
                && ("authoritative=false".equals(redirected.getRawQuery())
                        || "authoritative=true".equals(redirected.getRawQuery()));
    }

    private static boolean isAdminPath(final URI uri) {
        final String path = uri.getRawPath();
        return path != null && path.startsWith("/admin/v2/");
    }
}

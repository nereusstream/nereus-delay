package com.nereusstream.delay.transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Captures real Pulsar managed-ledger metadata around a Broker-1 failover. */
public final class PulsarClientArtifactBrokerProcessCrashStateSmoke {
    private static final Pattern LEDGER_ID = Pattern.compile("\\\"ledgerId\\\"\\s*:\\s*(-?\\d+)");
    private static final Pattern LEDGER_RECORD =
            Pattern.compile("\\\"ledgerId\\\"\\s*:\\s*(-?\\d+)\\s*,\\s*\\\"entries\\\"\\s*:\\s*(\\d+)");
    private static final int ADMIN_REQUEST_ATTEMPTS = 60;
    private static final long ADMIN_REQUEST_RETRY_MILLIS = 1_000L;

    private PulsarClientArtifactBrokerProcessCrashStateSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <admin-url> <topic>");
        }
        final String cell =
                valueOrDefault("NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_CELL", "pulsar-multi-broker-process-crash");
        final String phase = requiredPhase();
        final String directoryValue = requiredEnvironment("NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR");
        final String endpointLabel = valueOrDefault("NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT", arguments[0]);
        final String adminUrl = trimTrailingSlash(arguments[0]);
        final String topic = arguments[1];
        final HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .build();
        final String statsUrl = adminUrl + "/admin/v2/persistent/public/default/" + topic + "/internalStats";
        AdminResponse statsResponse = request(client, statsUrl + "?metadata=true");
        String adminReadPath = "internalStats?metadata=true";
        State state;
        if (statsResponse.statusCode() == 404) {
            statsResponse = request(client, statsUrl + "?metadata=false");
            adminReadPath = "internalStats?metadata=false-after-404";
            if (statsResponse.statusCode() == 404) {
                final AdminResponse managedLedgerInfo =
                        request(client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "/internal-info");
                requireSuccessful(
                        managedLedgerInfo,
                        adminUrl + "/admin/v2/persistent/public/default/" + topic + "/internal-info");
                state = readManagedLedgerInfo(managedLedgerInfo.body());
                adminReadPath = "internal-info-after-internalStats-404";
            } else {
                requireSuccessful(statsResponse, statsUrl);
                state = readState(statsResponse.body());
            }
        } else {
            requireSuccessful(statsResponse, statsUrl);
            state = readState(statsResponse.body());
        }
        final AdminResponse ready = request(client, adminUrl + "/admin/v2/brokers/ready");
        requireSuccessful(ready, adminUrl + "/admin/v2/brokers/ready");
        if (ready.body().isBlank()) {
            throw new IllegalStateException("Pulsar admin readiness response was empty: " + endpointLabel);
        }
        if (state.numberOfEntries() < 1
                || state.ledgerIds().isEmpty()
                || state.lastConfirmedLedger() < 0
                || state.lastConfirmedEntry() < 0) {
            throw new IllegalStateException("Pulsar internalStats did not expose a durable record: " + state);
        }
        writeStateDump(directoryValue, cell, phase, topic, endpointLabel, adminReadPath, state);
        System.out.println("Pulsar Broker " + cell + " durable state dump passed: phase=" + phase
                + ", topic=" + topic + ", endpoint=" + endpointLabel
                + ", readPath=" + adminReadPath
                + ", ledgerIds=" + state.ledgerIds()
                + ", numberOfEntries=" + state.numberOfEntries()
                + ", lastConfirmedEntry=" + state.lastConfirmedLedger() + ":" + state.lastConfirmedEntry());
    }

    private static String requiredPhase() {
        final String phase = requiredEnvironment("NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE");
        if (!phase.equals("before") && !phase.equals("after")) {
            throw new IllegalArgumentException("NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE must be before|after");
        }
        return phase;
    }

    private static AdminResponse request(final HttpClient client, final String url) throws Exception {
        AdminResponse lastResponse = null;
        Exception lastFailure = null;
        for (int attempt = 1; attempt <= ADMIN_REQUEST_ATTEMPTS; attempt++) {
            try {
                final HttpResponse<String> response = client.send(
                        HttpRequest.newBuilder(URI.create(url))
                                .timeout(java.time.Duration.ofSeconds(20))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                lastResponse = new AdminResponse(response.statusCode(), response.body());
                if (response.statusCode() != 500
                        && response.statusCode() != 502
                        && response.statusCode() != 503
                        && response.statusCode() != 504) {
                    return lastResponse;
                }
            } catch (java.io.IOException failure) {
                lastFailure = failure;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            if (attempt < ADMIN_REQUEST_ATTEMPTS) {
                Thread.sleep(ADMIN_REQUEST_RETRY_MILLIS);
            }
        }
        if (lastResponse != null) {
            return lastResponse;
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        throw new IllegalStateException("Pulsar admin request produced no response: " + url);
    }

    private static void requireSuccessful(final AdminResponse response, final String url) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException(
                    "Pulsar admin request failed: " + response.statusCode() + " " + url + " body=" + response.body());
        }
    }

    private static State readState(final String stats) {
        final List<Long> ledgerIds = new ArrayList<>(new TreeSet<>(allLongs(stats, LEDGER_ID)));
        final long numberOfEntries = requiredLong(stats, "numberOfEntries");
        final String lastConfirmed = requiredString(stats, "lastConfirmedEntry");
        final int separator = lastConfirmed.indexOf(':');
        if (separator <= 0 || separator == lastConfirmed.length() - 1) {
            throw new IllegalStateException("invalid lastConfirmedEntry: " + lastConfirmed);
        }
        final long lastConfirmedLedger = Long.parseLong(lastConfirmed.substring(0, separator));
        final long lastConfirmedEntry = Long.parseLong(lastConfirmed.substring(separator + 1));
        return new State(
                ledgerIds, numberOfEntries, lastConfirmedLedger, lastConfirmedEntry, requiredString(stats, "state"));
    }

    private static State readManagedLedgerInfo(final String managedLedgerInfo) {
        final TreeSet<Long> ledgerIds = new TreeSet<>();
        long numberOfEntries = 0;
        long lastConfirmedLedger = -1;
        long lastConfirmedEntry = -1;
        final Matcher matcher = LEDGER_RECORD.matcher(managedLedgerInfo);
        while (matcher.find()) {
            final long ledgerId = Long.parseLong(matcher.group(1));
            final long entries = Long.parseLong(matcher.group(2));
            ledgerIds.add(ledgerId);
            numberOfEntries += entries;
            if (ledgerId >= 0
                    && entries > 0
                    && (ledgerId > lastConfirmedLedger
                            || (ledgerId == lastConfirmedLedger && entries - 1 > lastConfirmedEntry))) {
                lastConfirmedLedger = ledgerId;
                lastConfirmedEntry = entries - 1;
            }
        }
        if (ledgerIds.isEmpty()) {
            throw new IllegalStateException("Pulsar internal-info did not expose ledger records: " + managedLedgerInfo);
        }
        return new State(
                new ArrayList<>(ledgerIds),
                numberOfEntries,
                lastConfirmedLedger,
                lastConfirmedEntry,
                "ManagedLedgerInfoRead");
    }

    private static List<Long> allLongs(final String input, final Pattern pattern) {
        final List<Long> values = new ArrayList<>();
        final Matcher matcher = pattern.matcher(input);
        while (matcher.find()) {
            values.add(Long.parseLong(matcher.group(1)));
        }
        return values;
    }

    private static long requiredLong(final String input, final String field) {
        final Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*(-?\\d+)")
                .matcher(input);
        if (!matcher.find()) {
            throw new IllegalStateException("missing numeric Pulsar internalStats field: " + field);
        }
        return Long.parseLong(matcher.group(1));
    }

    private static String requiredString(final String input, final String field) {
        final Matcher matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"")
                .matcher(input);
        if (!matcher.find()) {
            throw new IllegalStateException("missing string Pulsar internalStats field: " + field);
        }
        return matcher.group(1);
    }

    private static void writeStateDump(
            final String directoryValue,
            final String cell,
            final String phase,
            final String topic,
            final String endpointLabel,
            final String adminReadPath,
            final State state)
            throws Exception {
        final Path directory = Path.of(directoryValue).toAbsolutePath().normalize();
        Files.createDirectories(directory);
        final String json = "{\n"
                + " \"schema\": \"nereus-delay-chaos-durable-state-dump\",\n"
                + " \"cell\": " + jsonString(cell) + ",\n"
                + " \"phase\": "
                + jsonString(
                        phase.equals("before") ? "PULSAR_BROKER_PROCESS_CRASH_READY" : "RECOVERED_AFTER_FRESH_PROCESS")
                + ",\n"
                + " \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + " \"topic\": " + jsonString(topic) + ",\n"
                + " \"physical_topic\": " + jsonString("persistent://public/default/" + topic) + ",\n"
                + " \"cluster_id\": \"standalone\",\n"
                + " \"admin_endpoint\": " + jsonString(endpointLabel) + ",\n"
                + " \"admin_read_path\": " + jsonString(adminReadPath) + ",\n"
                + " \"ledger_ids\": " + state.ledgerIds() + ",\n"
                + " \"number_of_entries\": " + state.numberOfEntries() + ",\n"
                + " \"last_confirmed_ledger\": " + state.lastConfirmedLedger() + ",\n"
                + " \"last_confirmed_entry\": " + state.lastConfirmedEntry() + ",\n"
                + " \"managed_ledger_state\": " + jsonString(state.managedLedgerState()) + ",\n"
                + " \"durable_broker_read\": true,\n"
                + " \"dump_forced\": true\n"
                + "}\n";
        final String fileName = phase.equals("before") ? "before-process-crash.json" : "after-fresh-process.json";
        final Path target = directory.resolve(fileName);
        try (var channel = java.nio.channels.FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static String requiredEnvironment(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for Pulsar Broker recovery evidence");
        }
        return value;
    }

    private static String valueOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String trimTrailingSlash(final String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private record State(
            List<Long> ledgerIds,
            long numberOfEntries,
            long lastConfirmedLedger,
            long lastConfirmedEntry,
            String managedLedgerState) {}

    private record AdminResponse(int statusCode, String body) {}
}

package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.SloFinalOutcomeV1;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloObjectiveNameV1;
import io.nereusstream.delay.protocol.SloPathV1;
import io.nereusstream.delay.protocol.SloPopulationV1;
import io.nereusstream.delay.protocol.SloSampleEventIdentityV1;
import io.nereusstream.delay.protocol.SloSampleFinalV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;
import io.nereusstream.delay.protocol.SloThresholdUnitV1;
import io.nereusstream.delay.protocol.SloTimeEndpointKindV1;
import io.nereusstream.delay.protocol.SloTimeEndpointV1;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Bounded, source-locked local capacity evidence for the real Store/SLO
 * boundaries.  This is an evidence producer, not a release certification:
 * it deliberately records unavailable platform authorities instead of
 * substituting host or guessed values for cgroup/rlimit observations.
 */
class BoundedCapacitySloProbeTest {
    private static final int[] PAYLOAD_SIZES = {256, 4_096, 65_536};
    private static final int PAYLOAD_VALUE_TYPE = 3;
    private static final int DEFAULT_PAYLOAD_RECORDS = 16;
    private static final int DEFAULT_SLO_SAMPLES = 24;
    private static final int MAX_SAMPLES = 4_096;
    private static final long MAX_SLO_BYTES = 8L * 1024 * 1024;
    private static final long SAMPLE_START_EPOCH_MS = 1_700_000_000_000L;

    @TempDir
    Path tempDirectory;

    @Test
    void writesAndReopensBoundedPayloadAndSloEvidence() throws Exception {
        final int payloadRecords = positiveInt("NEREUS_DELAY_CAPACITY_PAYLOAD_RECORDS",
                DEFAULT_PAYLOAD_RECORDS, MAX_SAMPLES);
        final int sloSamples = positiveInt("NEREUS_DELAY_CAPACITY_SLO_SAMPLES",
                DEFAULT_SLO_SAMPLES, MAX_SAMPLES);
        final Path storeRoot = tempDirectory.resolve("store");
        final Path collectorState = tempDirectory.resolve("collector").resolve("state.bin");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final int outboxMaxRecords = Math.max(512, sloSamples + 1);
        final SloObservationOutboxLimits outboxLimits = new SloObservationOutboxLimits(
                outboxMaxRecords, MAX_SLO_BYTES);
        final SloObservationOutboxExportRate exportRate = new SloObservationOutboxExportRate(
                new SloObservationOutboxExportRate.Limits(outboxMaxRecords, MAX_SLO_BYTES));
        final List<PayloadRun> payloadRuns = new ArrayList<>();
        final List<SloObservationOutboxV1> exportedSamples;
        final SloObservationOutboxStore.Usage outboxUsage;
        final SloObservationOutboxExportRate.Usage exportUsage;
        final RocksDbUsageSnapshot usageBefore;
        final RocksDbUsageSnapshot usageAfter;
        final long sloElapsedNanos;

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store, outboxLimits,
                    exportRate);
            usageBefore = store.physicalUsage();
            for (int payloadSize : PAYLOAD_SIZES) {
                payloadRuns.add(writePayloads(store, payloadSize, payloadRecords));
            }
            final long sloStartNanos = System.nanoTime();
            for (int sample = 0; sample < sloSamples; sample++) {
                final SloSampleStartV1 start = sampleStart(sample);
                outbox.ensureStart(start);
                outbox.mergeFinal(sampleFinal(start, sample), SloThresholdDirectionV1.AT_MOST);
            }
            sloElapsedNanos = Math.max(1L, System.nanoTime() - sloStartNanos);
            exportedSamples = outbox.scan(outboxMaxRecords, MAX_SLO_BYTES);
            outboxUsage = outbox.usage();
            exportUsage = exportRate.usage();
            usageAfter = store.physicalUsage();
            assertEquals(sloSamples, exportedSamples.size());
            assertEquals(sloSamples, outboxUsage.recordCount());
            writeCollectorEvidence(collectorState, exportedSamples);
            writeArtifact(config, shardId, payloadRecords, sloSamples, payloadRuns, sloElapsedNanos,
                    usageBefore, usageAfter, outboxUsage, exportUsage, collectorState);
        }

        reopenAndVerify(config, shardId, payloadRecords, exportedSamples);
        verifyCollectorReopen(collectorState, exportedSamples);
        rewriteArtifactWithReopenEvidence(payloadRecords, sloSamples, payloadRuns, sloElapsedNanos, usageBefore,
                usageAfter, outboxUsage, exportUsage, collectorState, config, shardId, exportedSamples);
    }

    private static PayloadRun writePayloads(final ShardStore store, final int payloadSize,
                                            final int records) {
        final byte[] payload = new byte[payloadSize];
        Arrays.fill(payload, (byte) (payloadSize ^ 0x5a));
        final long startNanos = System.nanoTime();
        for (int record = 0; record < records; record++) {
            final byte[] key = payloadKey(payloadSize, record);
            store.write(batch -> batch.putValue(ColumnFamily.TIMELINE, PAYLOAD_VALUE_TYPE, key, payload));
        }
        final long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);
        for (int record = 0; record < records; record++) {
            final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.TIMELINE,
                    payloadKey(payloadSize, record), PAYLOAD_VALUE_TYPE);
            assertNotNull(value);
            assertArrayEquals(payload, value.payload());
        }
        final long inputBytes = Math.multiplyExact((long) payloadSize, records);
        final long recordsPerSecond = Math.round(records * 1_000_000_000.0d / elapsedNanos);
        return new PayloadRun(payloadSize, records, inputBytes, elapsedNanos, records, recordsPerSecond);
    }

    private static void writeCollectorEvidence(final Path stateFile,
                                               final List<SloObservationOutboxV1> samples) {
        final PersistentSloObservationCollector collector = new PersistentSloObservationCollector(stateFile,
                new SloObservationCollectorLimits(Math.max(512, samples.size() + 1), MAX_SLO_BYTES));
        for (SloObservationOutboxV1 sample : samples) {
            collector.merge(sample, SloThresholdDirectionV1.AT_MOST);
        }
        assertEquals(samples.size(), collector.size());
    }

    private static void reopenAndVerify(final ShardStoreConfig config, final ShardId shardId,
                                        final int payloadRecords,
                                        final List<SloObservationOutboxV1> expectedSamples) {
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(reopened,
                    new SloObservationOutboxLimits(Math.max(512, expectedSamples.size() + 1), MAX_SLO_BYTES));
            final List<SloObservationOutboxV1> actualSamples = outbox.scan(
                    Math.max(512, expectedSamples.size() + 1), MAX_SLO_BYTES);
            assertEquals(expectedSamples.size(), actualSamples.size());
            for (int payloadSize : PAYLOAD_SIZES) {
                final ValueEnvelope.Decoded value = reopened.getValue(ColumnFamily.TIMELINE,
                        payloadKey(payloadSize, payloadRecords - 1), PAYLOAD_VALUE_TYPE);
                assertNotNull(value);
                assertEquals(payloadSize, value.payload().length);
            }
            for (int index = 0; index < expectedSamples.size(); index++) {
                assertArrayEquals(expectedSamples.get(index).canonicalBytes(),
                        actualSamples.get(index).canonicalBytes());
            }
        }
    }

    private static void verifyCollectorReopen(final Path stateFile,
                                              final List<SloObservationOutboxV1> expectedSamples) {
        final PersistentSloObservationCollector reopened = new PersistentSloObservationCollector(stateFile,
                new SloObservationCollectorLimits(Math.max(512, expectedSamples.size() + 1), MAX_SLO_BYTES));
        assertEquals(expectedSamples.size(), reopened.size());
        for (SloObservationOutboxV1 expected : expectedSamples) {
            final SloObservationOutboxV1 actual = reopened.get(expected.sampleId());
            assertNotNull(actual);
            assertArrayEquals(expected.canonicalBytes(), actual.canonicalBytes());
        }
    }

    private static void writeArtifact(final ShardStoreConfig config, final ShardId shardId,
                                      final int payloadRecords, final int sloSamples,
                                      final List<PayloadRun> payloadRuns, final long sloElapsedNanos,
                                      final RocksDbUsageSnapshot usageBefore,
                                      final RocksDbUsageSnapshot usageAfter,
                                      final SloObservationOutboxStore.Usage outboxUsage,
                                      final SloObservationOutboxExportRate.Usage exportUsage,
                                      final Path collectorState) throws IOException {
        final Path artifact = artifactPath();
        Files.createDirectories(artifact.getParent());
        Files.writeString(artifact, artifactJson(config, shardId, payloadRecords, sloSamples, payloadRuns,
                sloElapsedNanos, usageBefore, usageAfter, outboxUsage, exportUsage, collectorState, false));
        System.out.println("Bounded capacity/SLO probe artifact: " + artifact.toAbsolutePath());
    }

    private static void rewriteArtifactWithReopenEvidence(final int payloadRecords, final int sloSamples,
                                                          final List<PayloadRun> payloadRuns,
                                                          final long sloElapsedNanos,
                                                          final RocksDbUsageSnapshot usageBefore,
                                                          final RocksDbUsageSnapshot usageAfter,
                                                          final SloObservationOutboxStore.Usage outboxUsage,
                                                          final SloObservationOutboxExportRate.Usage exportUsage,
                                                          final Path collectorState,
                                                          final ShardStoreConfig config,
                                                          final ShardId shardId,
                                                          final List<SloObservationOutboxV1> samples)
            throws IOException {
        final Path artifact = artifactPath();
        Files.writeString(artifact, artifactJson(config, shardId, payloadRecords, sloSamples, payloadRuns,
                sloElapsedNanos, usageBefore, usageAfter, outboxUsage, exportUsage, collectorState, true));
        System.out.println("Bounded capacity/SLO probe passed: samples=" + samples.size()
                + ", artifact=" + artifact.toAbsolutePath());
    }

    private static String artifactJson(final ShardStoreConfig config, final ShardId shardId,
                                       final int payloadRecords, final int sloSamples,
                                       final List<PayloadRun> payloadRuns, final long sloElapsedNanos,
                                       final RocksDbUsageSnapshot usageBefore,
                                       final RocksDbUsageSnapshot usageAfter,
                                       final SloObservationOutboxStore.Usage outboxUsage,
                                       final SloObservationOutboxExportRate.Usage exportUsage,
                                       final Path collectorState, final boolean reopenVerified) {
        final StringBuilder json = new StringBuilder(8_192);
        json.append("{\n");
        field(json, "schema", "nereus-delay-bounded-capacity-slo-probe-v1", true);
        field(json, "status", "PARTIAL", true);
        field(json, "source_lock", envOr("NEREUS_DELAY_CAPACITY_SOURCE_LOCK", "unspecified"), true);
        field(json, "started_at", Instant.now().toString(), true);
        field(json, "platform", System.getProperty("os.name") + " " + System.getProperty("os.arch"), true);
        field(json, "java_runtime", System.getProperty("java.runtime.version"), true);
        json.append("  \"configuration\": {\n");
        numberField(json, "payload_records_per_size", payloadRecords, true);
        numberField(json, "slo_samples", sloSamples, true);
        json.append("    \"payload_sizes_bytes\": [256, 4096, 65536],\n");
        numberField(json, "slo_outbox_max_records", Math.max(512, sloSamples + 1), true);
        numberField(json, "slo_outbox_max_bytes", MAX_SLO_BYTES, false);
        json.append("  },\n");
        json.append("  \"platform_probe\": ").append(platformProbeJson(config.rootPath())).append(",\n");
        json.append("  \"store\": {\n");
        field(json, "shard_id", shardId.routeIncarnation().uuid() + "/" + shardId.partition(), true);
        numberField(json, "max_open_shard_dbs", config.maxOpenShardDbs(), true);
        numberField(json, "max_total_open_files", config.maxTotalOpenFiles(), true);
        numberField(json, "shared_block_cache_bytes", config.sharedBlockCacheBytes(), true);
        numberField(json, "shared_write_buffer_budget_bytes", config.sharedWriteBufferBudgetBytes(), true);
        json.append("    \"payload_runs\": [\n");
        for (int index = 0; index < payloadRuns.size(); index++) {
            final PayloadRun run = payloadRuns.get(index);
            json.append("      {");
            numberFieldInline(json, "payload_bytes", run.payloadBytes(), true);
            numberFieldInline(json, "records", run.records(), true);
            numberFieldInline(json, "input_bytes", run.inputBytes(), true);
            numberFieldInline(json, "elapsed_nanos", run.elapsedNanos(), true);
            numberFieldInline(json, "verified_records", run.verifiedRecords(), true);
            numberFieldInline(json, "records_per_second", run.recordsPerSecond(), false);
            json.append("}").append(index + 1 == payloadRuns.size() ? "\n" : ",\n");
        }
        json.append("    ],\n");
        usageJson(json, "usage_before", usageBefore, true);
        usageJson(json, "usage_after", usageAfter, true);
        booleanField(json, "reopen_verified", reopenVerified, false);
        json.append("  },\n");
        json.append("  \"slo\": {\n");
        numberField(json, "durable_start_final_samples", sloSamples, true);
        numberField(json, "elapsed_nanos", sloElapsedNanos, true);
        numberField(json, "outbox_record_count", outboxUsage.recordCount(), true);
        numberField(json, "outbox_encoded_bytes", outboxUsage.encodedBytes(), true);
        numberField(json, "exported_records", exportUsage.records(), true);
        numberField(json, "exported_bytes", exportUsage.bytes(), true);
        numberField(json, "collector_state_bytes", fileSize(collectorState), true);
        booleanField(json, "collector_reopen_verified", reopenVerified, false);
        json.append("  },\n");
        json.append("  \"boundaries\": [\n");
        stringItem(json, "This is a bounded local evidence artifact, not a V1 benchmark or capacity certification.",
                true);
        stringItem(json, "It covers real synchronous RocksDB writes, payload readback, durable SLO outbox merge, and persistent collector reopen.",
                true);
        stringItem(json, "It does not cover Kafka/Pulsar/Gateway throughput, Broker batching/linger, 1M/10M/100M records, Lane distributions, multi-Worker placement, checkpoint restore throughput, or long-cycle soak.",
                true);
        stringItem(json, "The platform probe is authoritative only when WorkerRuntimeResourceProbe can read bounded JVM, procfs, cgroup, rlimit and filesystem sources; unavailable sources remain PARTIAL.",
                false);
        json.append("  ]\n");
        json.append("}\n");
        return json.toString();
    }

    private static String platformProbeJson(final Path rootPath) {
        final StringBuilder json = new StringBuilder(512);
        try {
            final WorkerRuntimeResourceObservation observation = WorkerRuntimeResourceProbe.observe(rootPath);
            json.append("{\"status\":\"AVAILABLE\",\"authority\":\"WorkerRuntimeResourceProbe\",");
            numberFieldInline(json, "jvm_heap_bytes", observation.actualJvmHeapBytes(), true);
            numberFieldInline(json, "direct_memory_bytes", observation.actualMaxDirectMemoryBytes(), true);
            numberFieldInline(json, "process_rss_bytes", observation.currentProcessRssBytes(), true);
            numberFieldInline(json, "cgroup_memory_limit_bytes", observation.effectiveCgroupMemoryLimitBytes(), true);
            numberFieldInline(json, "max_open_files", observation.maxProcessOpenFiles(), true);
            numberFieldInline(json, "current_open_files", observation.currentProcessOpenFiles(), true);
            numberFieldInline(json, "filesystem_bytes", observation.maxFilesystemBytes(), true);
            numberFieldInline(json, "usable_filesystem_bytes", observation.usableFilesystemBytes(), false);
            return json.append("}").toString();
        } catch (RuntimeException failure) {
            json.append("{\"status\":\"UNAVAILABLE\",\"authority\":\"WorkerRuntimeResourceProbe\",");
            fieldInline(json, "reason", boundedFailure(failure), false);
            return json.append("}").toString();
        }
    }

    private static void usageJson(final StringBuilder json, final String name,
                                  final RocksDbUsageSnapshot usage, final boolean trailingComma) {
        json.append("    ").append(jsonString(name)).append(": {");
        numberFieldInline(json, "live_sst_bytes", usage.liveSstBytes(), true);
        numberFieldInline(json, "wal_bytes", usage.walBytes(), true);
        numberFieldInline(json, "manifest_bytes", usage.manifestBytes(), true);
        numberFieldInline(json, "local_bytes", usage.localBytes(), true);
        numberFieldInline(json, "compaction_pending_bytes", usage.compactionPendingBytes(), true);
        numberFieldInline(json, "live_sst_files", usage.liveSstFiles(), true);
        numberFieldInline(json, "wal_files", usage.walFiles(), true);
        numberFieldInline(json, "local_files", usage.localFiles(), true);
        numberFieldInline(json, "l0_files", usage.l0Files(), false);
        json.append("}").append(trailingComma ? ",\n" : "\n");
    }

    private static SloSampleStartV1 sampleStart(final int seed) {
        final byte[] commandHash = bytes(32, seed + 1);
        final byte[] physicalAttemptId = bytes(16, seed + 2);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, seed + 3));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.COMMAND_QUEUED_LATENCY, branch);
        final long startEpoch = SAMPLE_START_EPOCH_MS + seed;
        return new SloSampleStartV1(bytes(32, seed + 4), SloObjectiveNameV1.COMMAND_QUEUED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloPathV1.NOT_APPLICABLE, identity,
                endpoint(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, startEpoch, seed + 5),
                startEpoch + 1_000);
    }

    private static SloSampleFinalV1 sampleFinal(final SloSampleStartV1 start, final int seed) {
        final long finalEpoch = start.start().earliestEpochMs() + 2;
        return new SloSampleFinalV1(start.sampleId(), start.startDigest(), SloFinalOutcomeV1.SUCCESS,
                SloThresholdUnitV1.MILLISECONDS, 1, 2, null,
                endpoint(SloTimeEndpointKindV1.BROKER_PERSISTENCE, finalEpoch, seed + 6),
                Bytes.sha256(Bytes.utf8("bounded-capacity-slo-evidence-" + seed)), 1);
    }

    private static SloTimeEndpointV1 endpoint(final SloTimeEndpointKindV1 kind, final long epoch,
                                              final int seed) {
        return new SloTimeEndpointV1(kind, epoch, epoch, bytes(32, seed));
    }

    private static byte[] payloadKey(final int payloadSize, final int record) {
        return Bytes.utf8("bounded-capacity/payload/" + payloadSize + "/" + record);
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static Path artifactPath() {
        final String configured = System.getenv("NEREUS_DELAY_CAPACITY_ARTIFACT_DIR");
        final Path directory = configured == null || configured.isBlank()
                ? Path.of("build", "reports", "bounded-capacity") : Path.of(configured);
        return directory.resolve("bounded-capacity-slo-probe.json");
    }

    private static long fileSize(final Path path) {
        try {
            return Files.size(path);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot inspect artifact file size", failure);
        }
    }

    private static int positiveInt(final String variable, final int defaultValue, final int max) {
        final String value = System.getenv(variable);
        final int parsed;
        try {
            parsed = value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(variable + " must be a bounded integer", failure);
        }
        if (parsed <= 0 || parsed > max) {
            throw new IllegalArgumentException(variable + " must be in (0," + max + "]");
        }
        return parsed;
    }

    private static String envOr(final String variable, final String fallback) {
        final String value = System.getenv(variable);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String boundedFailure(final RuntimeException failure) {
        final String message = failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage());
        return message.length() <= 256 ? message : message.substring(0, 256);
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\r", "\\r").replace("\n", "\\n") + "\"";
    }

    private static void field(final StringBuilder json, final String name, final String value,
                              final boolean trailingComma) {
        json.append("  ").append(jsonString(name)).append(":").append(jsonString(value));
        json.append(trailingComma ? ",\n" : "\n");
    }

    private static void fieldInline(final StringBuilder json, final String name, final String value,
                                    final boolean trailingComma) {
        json.append(jsonString(name)).append(":").append(jsonString(value));
        json.append(trailingComma ? "," : "");
    }

    private static void numberField(final StringBuilder json, final String name, final long value,
                                    final boolean trailingComma) {
        json.append("    ").append(jsonString(name)).append(":").append(value);
        json.append(trailingComma ? ",\n" : "\n");
    }

    private static void booleanField(final StringBuilder json, final String name, final boolean value,
                                     final boolean trailingComma) {
        json.append("    ").append(jsonString(name)).append(":").append(value);
        json.append(trailingComma ? ",\n" : "\n");
    }

    private static void numberFieldInline(final StringBuilder json, final String name, final long value,
                                          final boolean trailingComma) {
        json.append(jsonString(name)).append(":").append(value);
        json.append(trailingComma ? "," : "");
    }

    private static void stringItem(final StringBuilder json, final String value, final boolean trailingComma) {
        json.append("    ").append(jsonString(value));
        json.append(trailingComma ? ",\n" : "\n");
    }

    private record PayloadRun(int payloadBytes, int records, long inputBytes, long elapsedNanos,
                              int verifiedRecords, long recordsPerSecond) {
    }
}

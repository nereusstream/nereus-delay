package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DueExclusionReason;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SloAuthoritativeStartFactory;
import com.nereusstream.delay.protocol.SloFinalOutcome;
import com.nereusstream.delay.protocol.SloObjective;
import com.nereusstream.delay.protocol.SloObjectiveName;
import com.nereusstream.delay.protocol.SloObservationOutbox;
import com.nereusstream.delay.protocol.SloPath;
import com.nereusstream.delay.protocol.SloPopulation;
import com.nereusstream.delay.protocol.SloSampleEventIdentity;
import com.nereusstream.delay.protocol.SloSampleFinal;
import com.nereusstream.delay.protocol.SloSampleStart;
import com.nereusstream.delay.protocol.SloThresholdDirection;
import com.nereusstream.delay.protocol.SloThresholdUnit;
import com.nereusstream.delay.protocol.SloTimeEndpoint;
import com.nereusstream.delay.protocol.SloTimeEndpointKind;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SloObservationOutboxStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void scanRejectsKeyValueSampleIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-key-fence"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final SloObservationOutbox outbox = SloObservationOutbox.open(start());
        final byte[] anotherSampleId = bytes(32, 9);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(
                    ColumnFamily.META,
                    SloObservationOutboxStore.VALUE_TYPE,
                    KeyCodec.metaSloOutbox(anotherSampleId),
                    outbox.canonicalBytes()));
            final SloObservationOutboxStore outboxStore = new SloObservationOutboxStore(store);

            assertThrows(IllegalStateException.class, () -> outboxStore.get(anotherSampleId));
            assertThrows(IllegalStateException.class, () -> outboxStore.scan(10));
        }
    }

    @Test
    void scanReturnsOnlyExactKeyValuePairing() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-scan"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final SloSampleStart sample = start();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outboxStore = new SloObservationOutboxStore(store);
            outboxStore.ensureStart(sample);

            assertEquals(1, outboxStore.scan(10).size());
            assertEquals(sample, outboxStore.scan(10).get(0).start());
            final long encodedBytes = ValueEnvelope.encode(
                            SloObservationOutboxStore.VALUE_TYPE,
                            SloObservationOutbox.open(sample).canonicalBytes())
                    .length;
            assertEquals(1, outboxStore.scan(10, encodedBytes).size());
            assertThrows(IllegalStateException.class, () -> outboxStore.scan(10, encodedBytes - 1));
        }
    }

    @Test
    void excludedFinalRequiresPairedHealthyObjectiveAtDurableBoundary() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-healthy-pair"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final SloObjective healthy = dueHealthyObjective();
        final SloSampleStart sample = dueAcceptedStart();
        final SloSampleFinal excluded = new SloSampleFinal(
                sample.sampleId(),
                sample.startDigest(),
                SloFinalOutcome.BAD_EVIDENCE_GAP,
                SloThresholdUnit.MILLISECONDS,
                1,
                2,
                DueExclusionReason.CAPACITY_GATED,
                endpoint(300),
                bytes(32, 8),
                1);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);
            outbox.ensureStart(sample);

            assertThrows(
                    IllegalArgumentException.class, () -> outbox.mergeFinal(excluded, SloThresholdDirection.AT_MOST));
            assertEquals(
                    excluded,
                    outbox.mergeFinal(excluded, SloThresholdDirection.AT_MOST, healthy, dueAcceptedObjective())
                            .finalObservation());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> outbox.mergeFinal(excluded, SloThresholdDirection.AT_MOST, healthy));
        }
    }

    @Test
    void configuredCapacityBoundsRecordsAndEncodedBytesBeforeWrite() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-capacity"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final SloSampleStart first = start();
        final long firstBytes = ValueEnvelope.encode(
                        SloObservationOutboxStore.VALUE_TYPE,
                        SloObservationOutbox.open(first).canonicalBytes())
                .length;
        final SloObservationOutboxLimits limits = new SloObservationOutboxLimits(1, firstBytes);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store, limits);
            outbox.ensureStart(first);
            assertEquals(new SloObservationOutboxStore.Usage(1, firstBytes), outbox.usage());

            assertThrows(IllegalStateException.class, () -> outbox.ensureStart(startWith(4)));
            final SloSampleFinal finalObservation = new SloSampleFinal(
                    first.sampleId(),
                    first.startDigest(),
                    SloFinalOutcome.SUCCESS,
                    SloThresholdUnit.MILLISECONDS,
                    1,
                    1,
                    null,
                    brokerEndpoint(200),
                    bytes(32, 5),
                    1);
            assertThrows(
                    IllegalStateException.class,
                    () -> outbox.mergeFinal(finalObservation, SloThresholdDirection.AT_MOST));
            assertNull(outbox.get(first.sampleId()).finalObservation());

            assertTrue(outbox.deleteAfterCollectorAck(
                    first.sampleId(), outbox.get(first.sampleId()).recordDigest()));
            outbox.ensureStart(startWith(4));
            assertEquals(
                    new SloObservationOutboxStore.Usage(
                            1,
                            ValueEnvelope.encode(
                                            SloObservationOutboxStore.VALUE_TYPE,
                                            SloObservationOutbox.open(startWith(4))
                                                    .canonicalBytes())
                                    .length),
                    outbox.usage());
        }
    }

    @Test
    void exportRateBoundsEachScanWindowAndResetsAfterOneSecond() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-rate"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 4);
        final SloSampleStart sample = start();
        final long encodedBytes = ValueEnvelope.encode(
                        SloObservationOutboxStore.VALUE_TYPE,
                        SloObservationOutbox.open(sample).canonicalBytes())
                .length;
        final AtomicLong nowNanos = new AtomicLong();
        final SloObservationOutboxExportRate rate = new SloObservationOutboxExportRate(
                new SloObservationOutboxExportRate.Limits(1, encodedBytes), nowNanos::get);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox =
                    new SloObservationOutboxStore(store, new SloObservationOutboxLimits(2, encodedBytes * 2), rate);
            outbox.ensureStart(sample);

            assertEquals(1, outbox.scan(2).size());
            assertThrows(IllegalStateException.class, () -> outbox.scan(2));
            nowNanos.set(1_000_000_000L);
            assertEquals(1, outbox.scan(2).size());
            assertEquals(new SloObservationOutboxExportRate.Usage(1, encodedBytes), rate.usage());
        }
    }

    @Test
    void reconcileDurableStartsSortsDeduplicatesAndPreservesExistingFinal() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-reconcile"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 5);
        final SloSampleStart first = startWith(1);
        final SloSampleStart second = startWith(4);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            final List<SloObservationOutbox> reconciled = outbox.reconcileDurableStarts(List.of(second, first, first));
            final List<SloSampleStart> expected = List.of(first, second).stream()
                    .sorted((left, right) -> Arrays.compareUnsigned(left.sampleId(), right.sampleId()))
                    .toList();
            assertEquals(
                    expected,
                    reconciled.stream().map(SloObservationOutbox::start).toList());

            final SloSampleFinal finalObservation = new SloSampleFinal(
                    first.sampleId(),
                    first.startDigest(),
                    SloFinalOutcome.SUCCESS,
                    SloThresholdUnit.MILLISECONDS,
                    1,
                    1,
                    null,
                    brokerEndpoint(200),
                    bytes(32, 5),
                    1);
            outbox.mergeFinal(finalObservation, SloThresholdDirection.AT_MOST);

            final List<SloObservationOutbox> retried = outbox.reconcileDurableStarts(List.of(first, second));
            assertEquals(
                    finalObservation,
                    retried.stream()
                            .filter(value -> Arrays.equals(value.sampleId(), first.sampleId()))
                            .findFirst()
                            .orElseThrow()
                            .finalObservation());
            assertEquals(2, outbox.usage().recordCount());
        }
    }

    @Test
    void reconcileRejectsConflictingStartsBeforeAnyWrite() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-reconcile-conflict"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 6);
        final SloSampleStart first = startWith(5, 100);
        final SloSampleStart conflicting = startWith(5, 101);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            assertThrows(IllegalStateException.class, () -> outbox.reconcileDurableStarts(List.of(first, conflicting)));
            assertEquals(new SloObservationOutboxStore.Usage(0, 0), outbox.usage());
            assertNull(outbox.get(first.sampleId()));
        }
    }

    @Test
    void reconcilePreflightsCapacityBeforeAtomicBatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-reconcile-capacity"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final SloSampleStart first = startWith(7);
        final SloSampleStart second = startWith(8);
        final long oneRecordBytes = ValueEnvelope.encode(
                        SloObservationOutboxStore.VALUE_TYPE,
                        SloObservationOutbox.open(first).canonicalBytes())
                .length;
        final SloObservationOutboxLimits limits = new SloObservationOutboxLimits(1, oneRecordBytes);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store, limits);

            assertThrows(IllegalStateException.class, () -> outbox.reconcileDurableStarts(List.of(first, second)));
            assertEquals(new SloObservationOutboxStore.Usage(0, 0), outbox.usage());
            assertNull(outbox.get(first.sampleId()));
            assertNull(outbox.get(second.sampleId()));
        }
    }

    @Test
    void reconcileInCallerBatchSharesBusinessCommitAndRollsBackTogether() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-caller-batch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 10);
        final SloSampleStart sample = startWith(10);
        final byte[] businessKey = new byte[] {0x55, 0x01};
        final byte[] businessValue = bytes(7, 42);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            assertThrows(
                    IllegalStateException.class,
                    () -> store.write(batch -> {
                        outbox.reconcileDurableStartsInBatch(batch, List.of(sample));
                        batch.put(ColumnFamily.GC, businessKey, businessValue);
                        throw new IllegalStateException("abort source apply");
                    }));
            assertNull(outbox.get(sample.sampleId()));
            assertNull(store.get(ColumnFamily.GC, businessKey));

            store.write(batch -> {
                outbox.reconcileDurableStartsInBatch(batch, List.of(sample));
                batch.put(ColumnFamily.GC, businessKey, businessValue);
            });
            assertEquals(sample, outbox.get(sample.sampleId()).start());
            assertArrayEquals(businessValue, store.get(ColumnFamily.GC, businessKey));
        }
    }

    @Test
    void reconcileInCallerBatchRejectsABatchFromAnotherShardStore() {
        final ShardStoreConfig localConfig = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-local-batch"));
        final ShardStoreConfig foreignConfig = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-foreign-batch"));
        final ShardId localShard = new ShardId(RouteIncarnation.random(), 11);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 12);
        final SloSampleStart sample = startWith(11);
        try (SharedRocksDbResources localResources = new SharedRocksDbResources(localConfig);
                ShardStore localStore = ShardStore.open(localConfig, localShard, localResources);
                SharedRocksDbResources foreignResources = new SharedRocksDbResources(foreignConfig);
                ShardStore foreignStore = ShardStore.open(foreignConfig, foreignShard, foreignResources)) {
            final SloObservationOutboxStore localOutbox = new SloObservationOutboxStore(localStore);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> foreignStore.write(
                            batch -> localOutbox.reconcileDurableStartsInBatch(batch, List.of(sample))));
            assertNull(localOutbox.get(sample.sampleId()));
            assertNull(foreignStore.getValue(
                    ColumnFamily.META,
                    KeyCodec.metaSloOutbox(sample.sampleId()),
                    SloObservationOutboxStore.VALUE_TYPE));
        }
    }

    @Test
    void typedAuthorityConvenienceUsesExactFactoryBranches() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-authority-factory"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shardId, "cluster-slo", UUID.randomUUID(), 9, null, 700);
        final SloObjective commandApplied = new SloObjective(
                SloObjectiveName.COMMAND_APPLIED_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                List.of(),
                7,
                bytes(32, 31));
        final SloObjective due = new SloObjective(
                SloObjectiveName.DUE_ADMISSION_LAG,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                List.of(),
                7,
                bytes(32, 32));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);
            final SloObservationOutbox applied = outbox.ensureCommandAppliedStart(commandApplied, source);
            final SloObservationOutbox dueStart =
                    outbox.ensureDueAdmissionStart(due, messageId, 0, SloPath.ORDINARY_MANAGED, 800, bytes(32, 33));

            assertEquals(applied, outbox.ensureCommandAppliedStart(commandApplied, source));
            assertEquals(
                    dueStart,
                    outbox.ensureDueAdmissionStart(due, messageId, 0, SloPath.ORDINARY_MANAGED, 800, bytes(32, 33)));
            assertEquals(2, outbox.usage().recordCount());

            final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 9);
            final KafkaSourcePosition foreignSource =
                    new KafkaSourcePosition(foreignShard, "cluster-slo", UUID.randomUUID(), 10, null, 701);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> outbox.ensureCommandAppliedStart(commandApplied, foreignSource));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> outbox.ensureDueAdmissionStart(
                            due, DelayMessageId.random(foreignShard), 0, SloPath.ORDINARY_MANAGED, 800, bytes(32, 33)));

            final SloSampleStart foreignApplied =
                    SloAuthoritativeStartFactory.commandApplied(commandApplied, foreignSource);
            final SloSampleStart foreignDue = SloAuthoritativeStartFactory.dueAdmission(
                    due, DelayMessageId.random(foreignShard), 0, SloPath.ORDINARY_MANAGED, 800, bytes(32, 33));
            assertThrows(IllegalArgumentException.class, () -> outbox.ensureStart(foreignApplied));
            assertThrows(IllegalArgumentException.class, () -> outbox.ensureStart(foreignDue));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> outbox.reconcileDurableStarts(List.of(foreignApplied, foreignDue)));
            assertEquals(2, outbox.usage().recordCount());
        }
    }

    @Test
    void readPathsRejectPersistedTypedStartFromAnotherShard() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-read-shard-fence"));
        final ShardId localShard = new ShardId(RouteIncarnation.random(), 13);
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 14);
        final KafkaSourcePosition foreignSource =
                new KafkaSourcePosition(foreignShard, "cluster-slo", UUID.randomUUID(), 12, null, 702);
        final SloObjective commandApplied = new SloObjective(
                SloObjectiveName.COMMAND_APPLIED_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                List.of(),
                7,
                bytes(32, 34));
        final SloSampleStart foreignStart = SloAuthoritativeStartFactory.commandApplied(commandApplied, foreignSource);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, localShard, resources)) {
            store.write(batch -> batch.putValue(
                    ColumnFamily.META,
                    SloObservationOutboxStore.VALUE_TYPE,
                    KeyCodec.metaSloOutbox(foreignStart.sampleId()),
                    SloObservationOutbox.open(foreignStart).canonicalBytes()));
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            assertThrows(IllegalArgumentException.class, () -> outbox.get(foreignStart.sampleId()));
            assertThrows(IllegalArgumentException.class, () -> outbox.scan(10));
            assertThrows(IllegalArgumentException.class, outbox::usage);
        }
    }

    private static SloSampleStart start() {
        return startWith(1);
    }

    private static SloSampleStart startWith(final int seed) {
        return startWith(seed, 100);
    }

    private static SloSampleStart startWith(final int seed, final long startEpoch) {
        final byte[] commandHash = bytes(32, seed + 1);
        final byte[] physicalAttemptId = bytes(16, seed + 2);
        final byte[] completeBranchPayload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, seed));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.COMMAND_QUEUED_LATENCY, completeBranchPayload);
        return new SloSampleStart(
                bytes(32, seed),
                SloObjectiveName.COMMAND_QUEUED_LATENCY,
                SloPopulation.ALL_ACCEPTED,
                SloPath.NOT_APPLICABLE,
                identity,
                endpoint(startEpoch),
                200L);
    }

    private static SloSampleStart dueAcceptedStart() {
        final SloSampleEventIdentity identity =
                new SloSampleEventIdentity(SloObjectiveName.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 4));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.uint64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPath.ORDINARY_MANAGED.wireValue());
                }));
        final SloObjective allAccepted = new SloObjective(
                SloObjectiveName.DUE_ADMISSION_LAG,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                java.util.List.of(),
                7,
                bytes(32, 23));
        return new SloSampleStart(allAccepted, SloPath.ORDINARY_MANAGED, identity, endpoint(100), 200L);
    }

    private static SloObjective dueAcceptedObjective() {
        return new SloObjective(
                SloObjectiveName.DUE_ADMISSION_LAG,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                List.of(),
                7,
                bytes(32, 23));
    }

    private static SloObjective dueHealthyObjective() {
        return new SloObjective(
                SloObjectiveName.DUE_ADMISSION_LAG,
                SloPopulation.HEALTHY,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                100,
                99,
                100,
                60_000,
                10,
                java.util.List.of(DueExclusionReason.CAPACITY_GATED),
                7,
                bytes(32, 23));
    }

    private static SloTimeEndpoint endpoint(final long epochMs) {
        return new SloTimeEndpoint(
                SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static SloTimeEndpoint brokerEndpoint(final long epochMs) {
        return new SloTimeEndpoint(SloTimeEndpointKind.BROKER_PERSISTENCE, epochMs, epochMs, bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}

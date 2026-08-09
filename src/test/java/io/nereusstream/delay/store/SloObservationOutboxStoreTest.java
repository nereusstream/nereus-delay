package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DueExclusionReasonV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.SloFinalOutcomeV1;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloObjectiveNameV1;
import io.nereusstream.delay.protocol.SloObjectiveV1;
import io.nereusstream.delay.protocol.SloPathV1;
import io.nereusstream.delay.protocol.SloPopulationV1;
import io.nereusstream.delay.protocol.SloSampleEventIdentityV1;
import io.nereusstream.delay.protocol.SloSampleFinalV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;
import io.nereusstream.delay.protocol.SloThresholdUnitV1;
import io.nereusstream.delay.protocol.SloTimeEndpointKindV1;
import io.nereusstream.delay.protocol.SloTimeEndpointV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SloObservationOutboxStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void scanRejectsKeyValueSampleIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-key-fence"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.open(start());
        final byte[] anotherSampleId = bytes(32, 9);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, SloObservationOutboxStore.VALUE_TYPE,
                    KeyCodec.metaSloOutbox(anotherSampleId), outbox.canonicalBytes()));
            final SloObservationOutboxStore outboxStore = new SloObservationOutboxStore(store);

            assertThrows(IllegalStateException.class, () -> outboxStore.get(anotherSampleId));
            assertThrows(IllegalStateException.class, () -> outboxStore.scan(10));
        }
    }

    @Test
    void scanReturnsOnlyExactKeyValuePairing() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-scan"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final SloSampleStartV1 sample = start();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outboxStore = new SloObservationOutboxStore(store);
            outboxStore.ensureStart(sample);

            assertEquals(1, outboxStore.scan(10).size());
            assertEquals(sample, outboxStore.scan(10).get(0).start());
            final long encodedBytes = ValueEnvelope.encode(SloObservationOutboxStore.VALUE_TYPE,
                    SloObservationOutboxV1.open(sample).canonicalBytes()).length;
            assertEquals(1, outboxStore.scan(10, encodedBytes).size());
            assertThrows(IllegalStateException.class, () -> outboxStore.scan(10, encodedBytes - 1));
        }
    }

    @Test
    void excludedFinalRequiresPairedHealthyObjectiveAtDurableBoundary() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-healthy-pair"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final SloObjectiveV1 healthy = dueHealthyObjective();
        final SloSampleStartV1 sample = dueAcceptedStart();
        final SloSampleFinalV1 excluded = new SloSampleFinalV1(sample.sampleId(), sample.startDigest(),
                SloFinalOutcomeV1.BAD_EVIDENCE_GAP, SloThresholdUnitV1.MILLISECONDS, 1, 2,
                DueExclusionReasonV1.CAPACITY_GATED, endpoint(300), bytes(32, 8), 1);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);
            outbox.ensureStart(sample);

            assertThrows(IllegalArgumentException.class,
                    () -> outbox.mergeFinal(excluded, SloThresholdDirectionV1.AT_MOST));
            assertEquals(excluded,
                    outbox.mergeFinal(excluded, SloThresholdDirectionV1.AT_MOST, healthy, dueAcceptedObjective())
                            .finalObservation());
            assertThrows(IllegalArgumentException.class,
                    () -> outbox.mergeFinal(excluded, SloThresholdDirectionV1.AT_MOST, healthy));
        }
    }

    @Test
    void configuredCapacityBoundsRecordsAndEncodedBytesBeforeWrite() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-capacity"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final SloSampleStartV1 first = start();
        final long firstBytes = ValueEnvelope.encode(SloObservationOutboxStore.VALUE_TYPE,
                SloObservationOutboxV1.open(first).canonicalBytes()).length;
        final SloObservationOutboxLimits limits = new SloObservationOutboxLimits(1, firstBytes);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store, limits);
            outbox.ensureStart(first);
            assertEquals(new SloObservationOutboxStore.Usage(1, firstBytes), outbox.usage());

            assertThrows(IllegalStateException.class, () -> outbox.ensureStart(startWith(4)));
            final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(first.sampleId(), first.startDigest(),
                    SloFinalOutcomeV1.SUCCESS, SloThresholdUnitV1.MILLISECONDS, 1, 1, null,
                    brokerEndpoint(200), bytes(32, 5), 1);
            assertThrows(IllegalStateException.class,
                    () -> outbox.mergeFinal(finalObservation, SloThresholdDirectionV1.AT_MOST));
            assertNull(outbox.get(first.sampleId()).finalObservation());

            assertTrue(outbox.deleteAfterCollectorAck(first.sampleId(), outbox.get(first.sampleId()).recordDigest()));
            outbox.ensureStart(startWith(4));
            assertEquals(new SloObservationOutboxStore.Usage(1,
                    ValueEnvelope.encode(SloObservationOutboxStore.VALUE_TYPE,
                            SloObservationOutboxV1.open(startWith(4)).canonicalBytes()).length), outbox.usage());
        }
    }

    @Test
    void exportRateBoundsEachScanWindowAndResetsAfterOneSecond() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-rate"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 4);
        final SloSampleStartV1 sample = start();
        final long encodedBytes = ValueEnvelope.encode(SloObservationOutboxStore.VALUE_TYPE,
                SloObservationOutboxV1.open(sample).canonicalBytes()).length;
        final AtomicLong nowNanos = new AtomicLong();
        final SloObservationOutboxExportRate rate = new SloObservationOutboxExportRate(
                new SloObservationOutboxExportRate.Limits(1, encodedBytes), nowNanos::get);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store,
                    new SloObservationOutboxLimits(2, encodedBytes * 2), rate);
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
        final SloSampleStartV1 first = startWith(1);
        final SloSampleStartV1 second = startWith(4);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            final List<SloObservationOutboxV1> reconciled = outbox.reconcileDurableStarts(
                    List.of(second, first, first));
            final List<SloSampleStartV1> expected = List.of(first, second).stream()
                    .sorted((left, right) -> Arrays.compareUnsigned(left.sampleId(), right.sampleId()))
                    .toList();
            assertEquals(expected, reconciled.stream().map(SloObservationOutboxV1::start).toList());

            final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(first.sampleId(), first.startDigest(),
                    SloFinalOutcomeV1.SUCCESS, SloThresholdUnitV1.MILLISECONDS, 1, 1, null,
                    brokerEndpoint(200), bytes(32, 5), 1);
            outbox.mergeFinal(finalObservation, SloThresholdDirectionV1.AT_MOST);

            final List<SloObservationOutboxV1> retried = outbox.reconcileDurableStarts(List.of(first, second));
            assertEquals(finalObservation, retried.stream()
                    .filter(value -> Arrays.equals(value.sampleId(), first.sampleId()))
                    .findFirst().orElseThrow().finalObservation());
            assertEquals(2, outbox.usage().recordCount());
        }
    }

    @Test
    void reconcileRejectsConflictingStartsBeforeAnyWrite() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-reconcile-conflict"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 6);
        final SloSampleStartV1 first = startWith(5, 100);
        final SloSampleStartV1 conflicting = startWith(5, 101);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            assertThrows(IllegalStateException.class,
                    () -> outbox.reconcileDurableStarts(List.of(first, conflicting)));
            assertEquals(new SloObservationOutboxStore.Usage(0, 0), outbox.usage());
            assertNull(outbox.get(first.sampleId()));
        }
    }

    @Test
    void reconcilePreflightsCapacityBeforeAtomicBatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-reconcile-capacity"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final SloSampleStartV1 first = startWith(7);
        final SloSampleStartV1 second = startWith(8);
        final long oneRecordBytes = ValueEnvelope.encode(SloObservationOutboxStore.VALUE_TYPE,
                SloObservationOutboxV1.open(first).canonicalBytes()).length;
        final SloObservationOutboxLimits limits = new SloObservationOutboxLimits(1, oneRecordBytes);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store, limits);

            assertThrows(IllegalStateException.class,
                    () -> outbox.reconcileDurableStarts(List.of(first, second)));
            assertEquals(new SloObservationOutboxStore.Usage(0, 0), outbox.usage());
            assertNull(outbox.get(first.sampleId()));
            assertNull(outbox.get(second.sampleId()));
        }
    }

    @Test
    void reconcileInCallerBatchSharesBusinessCommitAndRollsBackTogether() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-caller-batch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 10);
        final SloSampleStartV1 sample = startWith(10);
        final byte[] businessKey = new byte[]{0x55, 0x01};
        final byte[] businessValue = bytes(7, 42);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);

            assertThrows(IllegalStateException.class, () -> store.write(batch -> {
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
        final SloSampleStartV1 sample = startWith(11);
        try (SharedRocksDbResources localResources = new SharedRocksDbResources(localConfig);
             ShardStore localStore = ShardStore.open(localConfig, localShard, localResources);
             SharedRocksDbResources foreignResources = new SharedRocksDbResources(foreignConfig);
             ShardStore foreignStore = ShardStore.open(foreignConfig, foreignShard, foreignResources)) {
            final SloObservationOutboxStore localOutbox = new SloObservationOutboxStore(localStore);

            assertThrows(IllegalArgumentException.class, () -> foreignStore.write(batch ->
                    localOutbox.reconcileDurableStartsInBatch(batch, List.of(sample))));
            assertNull(localOutbox.get(sample.sampleId()));
            assertNull(foreignStore.getValue(ColumnFamily.META, KeyCodec.metaSloOutbox(sample.sampleId()),
                    SloObservationOutboxStore.VALUE_TYPE));
        }
    }

    @Test
    void typedAuthorityConvenienceUsesExactFactoryBranches() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-authority-factory"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final KafkaSourcePosition source = new KafkaSourcePosition(shardId, "cluster-slo", UUID.randomUUID(), 9,
                null, 700);
        final SloObjectiveV1 commandApplied = new SloObjectiveV1(SloObjectiveNameV1.COMMAND_APPLIED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10, List.of(), 7, bytes(32, 31));
        final SloObjectiveV1 due = new SloObjectiveV1(SloObjectiveNameV1.DUE_ADMISSION_LAG,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10, List.of(), 7, bytes(32, 32));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);
            final SloObservationOutboxV1 applied = outbox.ensureCommandAppliedStart(commandApplied, source);
            final SloObservationOutboxV1 dueStart = outbox.ensureDueAdmissionStart(due, messageId, 0,
                    SloPathV1.ORDINARY_MANAGED, 800, bytes(32, 33));

            assertEquals(applied, outbox.ensureCommandAppliedStart(commandApplied, source));
            assertEquals(dueStart, outbox.ensureDueAdmissionStart(due, messageId, 0,
                    SloPathV1.ORDINARY_MANAGED, 800, bytes(32, 33)));
            assertEquals(2, outbox.usage().recordCount());

            final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 9);
            final KafkaSourcePosition foreignSource = new KafkaSourcePosition(foreignShard, "cluster-slo",
                    UUID.randomUUID(), 10, null, 701);
            assertThrows(IllegalArgumentException.class,
                    () -> outbox.ensureCommandAppliedStart(commandApplied, foreignSource));
            assertThrows(IllegalArgumentException.class,
                    () -> outbox.ensureDueAdmissionStart(due, DelayMessageId.random(foreignShard), 0,
                            SloPathV1.ORDINARY_MANAGED, 800, bytes(32, 33)));
            assertEquals(2, outbox.usage().recordCount());
        }
    }

    private static SloSampleStartV1 start() {
        return startWith(1);
    }

    private static SloSampleStartV1 startWith(final int seed) {
        return startWith(seed, 100);
    }

    private static SloSampleStartV1 startWith(final int seed, final long startEpoch) {
        final byte[] commandHash = bytes(32, seed + 1);
        final byte[] physicalAttemptId = bytes(16, seed + 2);
        final byte[] completeBranchPayload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, seed));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.COMMAND_QUEUED_LATENCY, completeBranchPayload);
        return new SloSampleStartV1(bytes(32, seed), SloObjectiveNameV1.COMMAND_QUEUED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloPathV1.NOT_APPLICABLE, identity,
                endpoint(startEpoch), 200L);
    }

    private static SloSampleStartV1 dueAcceptedStart() {
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.DUE_ADMISSION_LAG, CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, bytes(41, 4));
                    CanonicalProtobuf.uint32(output, 2, 1);
                    CanonicalProtobuf.uint64(output, 3, 100);
                    CanonicalProtobuf.uint32(output, 4, SloPathV1.ORDINARY_MANAGED.wireValue());
                }));
        final SloObjectiveV1 allAccepted = new SloObjectiveV1(SloObjectiveNameV1.DUE_ADMISSION_LAG,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10, java.util.List.of(), 7,
                bytes(32, 23));
        return new SloSampleStartV1(allAccepted, SloPathV1.ORDINARY_MANAGED, identity,
                endpoint(100), 200L);
    }

    private static SloObjectiveV1 dueAcceptedObjective() {
        return new SloObjectiveV1(SloObjectiveNameV1.DUE_ADMISSION_LAG,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10, List.of(), 7, bytes(32, 23));
    }

    private static SloObjectiveV1 dueHealthyObjective() {
        return new SloObjectiveV1(SloObjectiveNameV1.DUE_ADMISSION_LAG, SloPopulationV1.HEALTHY,
                SloThresholdDirectionV1.AT_MOST, SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10,
                java.util.List.of(DueExclusionReasonV1.CAPACITY_GATED), 7, bytes(32, 23));
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static SloTimeEndpointV1 brokerEndpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.BROKER_PERSISTENCE, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}

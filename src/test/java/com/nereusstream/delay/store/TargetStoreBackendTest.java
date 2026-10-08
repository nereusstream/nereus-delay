package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetQuotaTotalsDelta;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.RocksDBException;

/** Minimal development smoke only; full business/authority/recovery validation remains in the handoff list. */
class TargetStoreBackendTest {
    @TempDir
    Path root;

    private final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
    private final byte[] incarnation = bytes(16, 0x11);
    private final TargetQuotaScope scope = new TargetQuotaScope(shard, bytes(32, 0x22), null);

    @Test
    void explicitReadersRejectTheOtherFormatWithoutConvertingItsIdentity() {
        final var targetConfig = ShardStoreConfig.defaults(root.resolve("target"));
        byte[] identity;
        try (var resources = new SharedRocksDbResources(targetConfig);
                var store = ShardStore.openTarget(targetConfig, shard, resources)) {
            identity = store.metadata().encode();
            assertEquals(2, store.metadata().storeFormatVersion());
        }
        try (var resources = new SharedRocksDbResources(targetConfig)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.open(targetConfig, shard, resources));
        }
        try (var resources = new SharedRocksDbResources(targetConfig);
                var store = ShardStore.openTarget(targetConfig, shard, resources)) {
            assertArrayEquals(identity, store.metadata().encode());
        }
        final var laneConfig = ShardStoreConfig.defaults(root.resolve("lane"));
        try (var resources = new SharedRocksDbResources(laneConfig);
                var store = ShardStore.open(laneConfig, shard, resources)) {
            identity = store.metadata().encode();
        }
        try (var resources = new SharedRocksDbResources(laneConfig)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.openTarget(laneConfig, shard, resources));
        }
        try (var resources = new SharedRocksDbResources(laneConfig);
                var store = ShardStore.open(laneConfig, shard, resources)) {
            assertArrayEquals(identity, store.metadata().encode());
        }
    }

    @Test
    void openingAnEmptyTargetStoreDoesNotReconstructOrActivateItsWorkerRoot() {
        final var config = ShardStoreConfig.defaults(root.resolve("target-open-is-not-activation"));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            assertEquals(2, store.metadata().storeFormatVersion());
            assertNull(store.appliedShardLogPosition());
            assertEquals(0, store.shardMutationSequence());
            assertEquals(0, store.runtimeMetadata().lastOpenedOwnerEpoch());

            final long beforeReopen = store.latestSequenceNumber();
            assertThrows(
                    IllegalStateException.class,
                    () -> TargetStoreBootstrap.reopen(
                            store,
                            scope,
                            new TargetStoreBackend.WriteLimits(64, 2 << 20),
                            new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                            (metadata, targetScope) -> guard()));

            assertEquals(beforeReopen, store.latestSequenceNumber());
            assertNull(store.appliedShardLogPosition());
            assertEquals(0, store.shardMutationSequence());
            assertEquals(0, store.runtimeMetadata().lastOpenedOwnerEpoch());
        }
    }

    @Test
    void sourceAndAggregateCommitOnceSurviveReopenAndFenceStalePlans() {
        final var config = ShardStoreConfig.defaults(root.resolve("atomic"));
        byte[] aggregate;
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            final var backend = backend(store);
            final var first = plan(backend, 1);
            final var stale = plan(backend, 1);
            final long before = store.operationStatistics().successfulWriteCalls();
            backend.commit(first, (metadata, targetScope, mutation) -> new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {}

                @Override
                public void close() {}
            });
            assertEquals(before + 1, store.operationStatistics().successfulWriteCalls());
            assertEquals(1, store.shardMutationSequence());
            assertArrayEquals(
                    source(1).canonicalBytes(), store.appliedShardLogPosition().canonicalBytes());
            assertThrows(IllegalStateException.class, () -> backend.commit(first, (a, b, c) -> guard()));
            assertThrows(IllegalStateException.class, () -> backend.commit(stale, (a, b, c) -> guard()));
            aggregate = store.get(
                    ColumnFamily.META,
                    TargetQuotaAggregate.genesis(shard, incarnation).key());
            assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(aggregate));
        }
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            assertEquals(1, store.shardMutationSequence());
            assertArrayEquals(
                    aggregate,
                    store.get(
                            ColumnFamily.META,
                            TargetQuotaAggregate.genesis(shard, incarnation).key()));
            final var backend = backend(store);
            final var next = plan(backend, 2);
            backend.commit(next, (a, b, c) -> guard());
            assertEquals(2, store.shardMutationSequence());
        }
    }

    @Test
    void completingAQuotaReadPlanAfterAnInterveningBatchRejectsItsStaleView() {
        final var config = ShardStoreConfig.defaults(root.resolve("stale-read-plan"));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            final var backend = backend(store);
            final var readPlan = backend.guardedPrepareRead(
                    new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                    reader -> reader.sourceSequence(),
                    (metadata, targetScope) -> guard());

            backend.commit(plan(backend, 1), (metadata, targetScope, mutation) -> guard());

            assertThrows(
                    IllegalStateException.class,
                    () -> backend.completeRead(readPlan, (metadata, targetScope) -> guard()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void quotaBusinessAndSourceRemainOneBatchAcrossNativeWriteFailure(final boolean writeBeforeFailure) {
        final var config = ShardStoreConfig.defaults(root.resolve("target-write-failure-" + writeBeforeFailure));
        final byte[] businessKey = TargetKeyCodec.message(DelayMessageId.random(shard));
        final byte[] businessValue = Bytes.utf8("target-atomic-value");
        final var cursor = EvidenceCursor.targetPulsar(new EvidenceCursor.TargetScope(shard,
                new TargetPartitionId(bytes(32, 0x43)), new TargetKeyCodec.Domain(1, 1), incarnation, bytes(32, 0x41)),
                bytes(32, 0x42), shard.partition(), 1, 100, "persistent://tenant/ns/target-cursor", 1, 2, 3, 0, 1);
        final TargetStoreBackend.Mutation mutation;
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            final var backend = backend(store);
            final var prepared = backend.prepare(
                    new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                    reader -> {
                        final var counters = TargetQuotaDelta.prepare(
                                reader.aggregate(),
                                reader.sourceSequence(),
                                reader.source(),
                                source(1),
                                bytes(32, 1),
                                List.of(),
                                4,
                                reader::counter);
                        final var totals = TargetQuotaTotalsDelta.prepare(counters, scope, 1, reader::total);
                        final var business = reader.replace(ColumnFamily.ID, businessKey, 15, businessValue);
                        return new TargetStoreBackend.Mutation(totals, List.of(business), null,
                                reader.advanceEvidenceCursor(cursor));
                    });
            mutation = prepared.mutation();
            final var failure = assertThrows(
                    ShardStore.RocksDbWriteFailure.class,
                    () -> backend.commit(
                            prepared,
                            (metadata, targetScope, actual) -> guard(),
                            (db, writeOptions, batch) -> {
                                if (writeBeforeFailure) {
                                    db.write(writeOptions, batch);
                                }
                                throw new RocksDBException("synthetic Target batch write response failure");
                            }));
            assertEquals("RocksDB write failed", failure.getMessage());
            assertTrue(store.isWriteOutcomeUncertain());
            assertThrows(IllegalStateException.class, () -> store.get(ColumnFamily.ID, businessKey));
        }

        try (var resources = new SharedRocksDbResources(config);
                var reopened = ShardStore.openTarget(config, shard, resources)) {
            if (writeBeforeFailure) {
                assertArrayEquals(
                        TargetValueEnvelope.encode(15, businessValue), reopened.get(ColumnFamily.ID, businessKey));
                assertEquals(1, reopened.shardMutationSequence());
                assertArrayEquals(source(1).canonicalBytes(), reopened.appliedShardLogPosition().canonicalBytes());
                assertEquals(List.of(cursor), reopened.runtimeMetadata().evidenceCursors());
            } else {
                assertNull(reopened.get(ColumnFamily.ID, businessKey));
                assertEquals(0, reopened.shardMutationSequence());
                assertNull(reopened.appliedShardLogPosition());
                assertTrue(reopened.runtimeMetadata().evidenceCursors().isEmpty());
            }
            for (var change : mutation.quota().counters().changes()) {
                final byte[] stored = reopened.get(ColumnFamily.META, change.next().identity().key());
                if (writeBeforeFailure) {
                    assertArrayEquals(
                            TargetValueEnvelope.encode(TargetQuotaCounter.VALUE_TYPE, change.next().canonicalBytes()),
                            stored);
                } else {
                    assertNull(stored);
                }
            }
            for (var change : mutation.quota().changes()) {
                final byte[] stored = reopened.get(ColumnFamily.META, change.next().key());
                if (writeBeforeFailure) {
                    assertArrayEquals(
                            TargetValueEnvelope.encode(TargetQuotaTotal.VALUE_TYPE, change.next().canonicalBytes()),
                            stored);
                } else {
                    assertNull(stored);
                }
            }
            final var aggregate = mutation.quota().counters().nextAggregate();
            final byte[] storedAggregate = reopened.get(ColumnFamily.META, aggregate.key());
            if (writeBeforeFailure) {
                assertArrayEquals(
                        TargetValueEnvelope.encode(TargetQuotaAggregate.VALUE_TYPE, aggregate.canonicalBytes()),
                        storedAggregate);
            } else {
                assertNull(storedAggregate);
            }
        }
    }

    @Test
    void cursorSeedRetainsLateFrontiersAndSeparateGenerations() {
        final var config = ShardStoreConfig.defaults(root.resolve("cursor-frontiers"));
        final var cursor = targetCursor(1, 8, 300);
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            store.recordEvidenceCursors(List.of(cursor));
            final var backend = backend(store);
            final long before = store.latestSequenceNumber();
            assertTrue(backend.prepareRead(new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                    reader -> reader.advanceEvidenceCursor(targetCursor(1, 3, 250)) == null).value());
            final var advance = backend.prepareRead(
                    new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                    reader -> reader.advanceEvidenceCursor(targetCursor(1, 9, 301))).value();
            assertEquals(List.of(targetCursor(1, 9, 301)), advance.after());
            final var generation = backend.prepareRead(
                    new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                    reader -> reader.advanceEvidenceCursor(targetCursor(2, 1, 1))).value();
            assertEquals(List.of(cursor, targetCursor(2, 1, 1)), generation.after());
            assertThrows(IllegalArgumentException.class, () -> backend.prepareRead(
                    new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime),
                    reader -> reader.advanceEvidenceCursor(targetCursor(1, 9, 200))));
            assertThrows(IllegalArgumentException.class,
                    () -> new TargetStoreBackend.EvidenceCursorChange(generation.encodedAfter(), List.of(cursor)));
            assertEquals(before, store.latestSequenceNumber());
            assertEquals(List.of(cursor), store.runtimeMetadata().evidenceCursors());
        }
    }

    private EvidenceCursor targetCursor(long generation, long entry, long timestamp) {
        return EvidenceCursor.targetPulsar(new EvidenceCursor.TargetScope(shard,
                new TargetPartitionId(bytes(32, 0x43)), new TargetKeyCodec.Domain(1, 1), incarnation, bytes(32, 0x41)),
                bytes(32, 0x42), shard.partition(), generation, timestamp,
                "persistent://tenant/ns/target-cursor", 1, 2, entry, 0, 1);
    }

    @Test
    void targetWakeRegistrationsCoalesceAndFallBackToBoundedInventoryDiscovery() throws Exception {
        final var signal = new TargetStoreBackend.TargetQueueChangeSignal();
        signal.configureTargetLimit(2);
        final var first = new TargetPartitionId(bytes(TargetPartitionId.LENGTH, 0x41));
        final var second = new TargetPartitionId(bytes(TargetPartitionId.LENGTH, 0x42));
        final var third = new TargetPartitionId(bytes(TargetPartitionId.LENGTH, 0x43));
        signal.registerTargets(List.of(first));

        final long beforeKnownTarget = signal.revision();
        signal.signalChanges(Set.of(first), false);
        signal.signalChanges(Set.of(first), false);
        assertTrue(signal.awaitChange(beforeKnownTarget, Duration.ZERO));
        final var knownChanges = signal.drainChanges();
        assertFalse(knownChanges.inventoryDirty());
        assertEquals(Set.of(first), knownChanges.dirtyTargets());

        signal.registerTargets(List.of(first, second));
        assertThrows(IllegalStateException.class, () -> signal.registerTargets(List.of(first, second, third)));
        signal.signalChanges(Set.of(second), true);
        final var joinedSourceChanges = signal.drainChanges();
        assertFalse(joinedSourceChanges.inventoryDirty());
        assertTrue(joinedSourceChanges.businessRecheck());
        assertEquals(Set.of(second), joinedSourceChanges.dirtyTargets());

        signal.signalChanges(Set.of(first), false);
        signal.unregisterTarget(first);
        final var concurrentRemoval = signal.drainChanges();
        assertTrue(concurrentRemoval.inventoryDirty());
        assertTrue(concurrentRemoval.dirtyTargets().isEmpty());

        signal.signalChanges(Set.of(third), false);
        final var unknownTarget = signal.drainChanges();
        assertTrue(unknownTarget.inventoryDirty());
        assertTrue(unknownTarget.dirtyTargets().isEmpty());

        final long quietRevision = signal.revision();
        assertFalse(signal.awaitChange(quietRevision, Duration.ofMillis(1)));
        assertEquals(quietRevision, signal.revision());
    }

    private TargetStoreBackend backend(final ShardStore store) {
        return new TargetStoreBackend(
                store, scope, incarnation, bytes(16, 0x33), new TargetStoreBackend.WriteLimits(16, 1 << 20));
    }

    private TargetStoreBackend.Prepared plan(final TargetStoreBackend backend, final int offset) {
        return backend.prepare(new BoundedReadBudget(100, 1 << 20, 10_000_000_000L, System::nanoTime), reader -> {
            final var counters = TargetQuotaDelta.prepare(
                    reader.aggregate(),
                    reader.sourceSequence(),
                    reader.source(),
                    source(offset),
                    bytes(32, offset),
                    List.of(),
                    4,
                    reader::counter);
            return new TargetStoreBackend.Mutation(
                    TargetQuotaTotalsDelta.prepare(counters, scope, 1, reader::total), List.of());
        });
    }

    private KafkaSourcePosition source(final long offset) {
        return new KafkaSourcePosition(shard, "target-store-smoke", new java.util.UUID(1, 2), offset, 1, offset);
    }

    private static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] raw = new byte[length];
        java.util.Arrays.fill(raw, (byte) value);
        return Bytes.copy(raw);
    }
}

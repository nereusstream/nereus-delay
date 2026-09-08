package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetQuotaTotalsDelta;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

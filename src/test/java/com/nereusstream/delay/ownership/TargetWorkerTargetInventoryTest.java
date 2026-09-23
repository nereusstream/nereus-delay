package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetWorkerTargetInventoryTest {
    private static final TargetWorkerTargetInventory.Limits LIMITS =
            new TargetWorkerTargetInventory.Limits(2, 2, 1, 3, 10, 1 << 20, 1_000_000_000L);

    @TempDir
    Path root;

    @Test
    void realStoresMergePhysicalTargetAndRejectAWriteBetweenPages() {
        final var config = ShardStoreConfig.defaults(root);
        final var firstShard = shard(1);
        final var secondShard = shard(2);
        final var firstTarget = target(0);
        final var secondTarget = target(1);
        try (var resources = new SharedRocksDbResources(config);
                var firstStore = ShardStore.openTarget(config, firstShard, resources);
                var secondStore = ShardStore.openTarget(config, secondShard, resources)) {
            seedQueue(firstStore, firstTarget, 1);
            seedQueue(firstStore, secondTarget, 1);
            seedQueue(secondStore, firstTarget, 1);
            final var firstSource = storeSource(firstStore);
            final var secondSource = storeSource(secondStore);
            final var complete = TargetWorkerTargetInventory.rebuild(
                    List.of(firstSource, secondSource), LIMITS, this::budget, () -> true);
            assertEquals(TargetWorkerTargetInventory.Stop.COMPLETE, complete.stop());
            assertEquals(2, complete.snapshot().targets().size());
            final var shared = complete.snapshot().targets().stream()
                    .filter(target -> target.id().equals(firstTarget.id()))
                    .findFirst()
                    .orElseThrow();
            assertEquals(
                    List.of(firstShard, secondShard),
                    shared.sources().stream()
                            .map(TargetWorkerTargetInventory.Source::shard)
                            .toList());
            assertEquals(
                    firstSource.readCut(budget()), complete.snapshot().cuts().get(firstShard));
            assertEquals(
                    secondSource.readCut(budget()), complete.snapshot().cuts().get(secondShard));

            final var beforeWrite = firstSource.readCut(budget());
            final var changed = new TargetWorkerTargetInventory.ShardSource() {
                private boolean written;

                @Override
                public ShardId shardId() {
                    return firstShard;
                }

                @Override
                public TargetQueueSnapshotReader.Page scan(
                        final BoundedReadBudget budget, final TargetPartitionId after, final int pageTargets) {
                    final var page = firstSource.scan(budget, after, pageTargets);
                    if (!written) {
                        written = true;
                        seedQueue(firstStore, firstTarget, 2);
                    }
                    return page;
                }

                @Override
                public TargetQueueSnapshotReader.Cut readCut(final BoundedReadBudget budget) {
                    return firstSource.readCut(budget);
                }
            };
            final var rejected = TargetWorkerTargetInventory.rebuild(
                    List.of(changed, secondSource), LIMITS, this::budget, () -> true);
            assertEquals(TargetWorkerTargetInventory.Stop.CUT_CHANGED, rejected.stop());
            assertNull(rejected.snapshot());
            assertNotEquals(beforeWrite, firstSource.readCut(budget()));
        }
    }

    @Test
    void mergesSamePhysicalTargetAcrossShardsOnlyAfterAllCutsRemainCurrent() {
        final var firstShard = shard(1);
        final var secondShard = shard(2);
        final var firstTarget = target(0);
        final var secondTarget = target(1);
        final var ordered = new ArrayList<>(List.of(firstTarget, secondTarget));
        ordered.sort((left, right) ->
                Arrays.compareUnsigned(left.id().bytes(), right.id().bytes()));
        final var firstCut = cut(1, 7);
        final var secondCut = cut(2, 11);
        final var first = new FakeSource(
                firstShard,
                List.of(
                        page(List.of(entry(ordered.get(0))), TargetQueueSnapshotReader.Stop.PAGE_LIMIT, firstCut),
                        page(List.of(entry(ordered.get(1))), TargetQueueSnapshotReader.Stop.RANGE_END, firstCut)),
                firstCut);
        final var second = new FakeSource(
                secondShard,
                List.of(page(List.of(entry(firstTarget)), TargetQueueSnapshotReader.Stop.RANGE_END, secondCut)),
                secondCut);

        final var result =
                TargetWorkerTargetInventory.rebuild(List.of(first, second), LIMITS, this::budget, () -> true);

        assertEquals(TargetWorkerTargetInventory.Stop.COMPLETE, result.stop());
        assertEquals(2, result.snapshot().targets().size());
        assertEquals(firstCut, result.snapshot().cuts().get(firstShard));
        assertEquals(secondCut, result.snapshot().cuts().get(secondShard));
        final var merged = result.snapshot().targets().stream()
                .filter(target -> target.id().equals(firstTarget.id()))
                .findFirst()
                .orElseThrow();
        assertEquals(
                List.of(firstShard, secondShard),
                merged.sources().stream()
                        .map(TargetWorkerTargetInventory.Source::shard)
                        .toList());
    }

    @Test
    void rejectsSourceChangesBetweenPagesAndAfterRangeEndWithoutPublishingPartialRing() {
        final var physical = target(0);
        final var before = cut(1, 7);
        final var after = cut(1, 8);
        final var changedPage = new FakeSource(
                shard(1),
                List.of(
                        page(List.of(entry(physical)), TargetQueueSnapshotReader.Stop.PAGE_LIMIT, before),
                        new TargetQueueSnapshotReader.Page(
                                List.of(), physical.id(), TargetQueueSnapshotReader.Stop.RANGE_END, after)),
                after);
        final var pageResult =
                TargetWorkerTargetInventory.rebuild(List.of(changedPage), LIMITS, this::budget, () -> true);
        assertEquals(TargetWorkerTargetInventory.Stop.CUT_CHANGED, pageResult.stop());
        assertNull(pageResult.snapshot());

        final var changedEnd = new FakeSource(
                shard(1),
                List.of(page(List.of(entry(physical)), TargetQueueSnapshotReader.Stop.RANGE_END, before)),
                after);
        final var endResult =
                TargetWorkerTargetInventory.rebuild(List.of(changedEnd), LIMITS, this::budget, () -> true);
        assertEquals(TargetWorkerTargetInventory.Stop.CUT_CHANGED, endResult.stop());
        assertNull(endResult.snapshot());
    }

    @Test
    void doesNotPublishAnIncompletePageOrChangedShardMembership() {
        final var before = cut(1, 7);
        final var incomplete = new FakeSource(
                shard(1), List.of(page(List.of(), TargetQueueSnapshotReader.Stop.READ_BUDGET, before)), before);
        final var readResult =
                TargetWorkerTargetInventory.rebuild(List.of(incomplete), LIMITS, this::budget, () -> true);
        assertEquals(TargetWorkerTargetInventory.Stop.READ_BUDGET, readResult.stop());
        assertNull(readResult.snapshot());

        final var complete = new FakeSource(
                shard(1), List.of(page(List.of(), TargetQueueSnapshotReader.Stop.RANGE_END, before)), before);
        final var membershipResult =
                TargetWorkerTargetInventory.rebuild(List.of(complete), LIMITS, this::budget, () -> false);
        assertEquals(TargetWorkerTargetInventory.Stop.MEMBERSHIP_CHANGED, membershipResult.stop());
        assertNull(membershipResult.snapshot());
    }

    private BoundedReadBudget budget() {
        return new BoundedReadBudget(10, 1 << 20, 1_000_000_000L, () -> 0);
    }

    private static void seedQueue(
            final ShardStore store, final CanonicalTargetPartition physical, final long headRevision) {
        final var queue = new TargetQueueState(
                physical.id(), headRevision, 1, TargetQueueState.AdmissionState.OPEN, bytes(16, 3), 0, List.of());
        store.write(batch -> {
            batch.put(
                    ColumnFamily.META,
                    TargetKeyCodec.identity(physical.id()),
                    TargetValueEnvelope.encode(CanonicalTargetPartition.VALUE_TYPE, physical.canonicalBytes()));
            batch.put(
                    ColumnFamily.META,
                    TargetKeyCodec.state(physical.id()),
                    TargetValueEnvelope.encode(TargetQueueState.VALUE_TYPE, queue.canonicalBytes()));
        });
    }

    private static TargetWorkerTargetInventory.ShardSource storeSource(final ShardStore store) {
        final var scope = new TargetQuotaScope(store.shardId(), bytes(32, 5), null);
        final var backend = new TargetStoreBackend(
                store, scope, bytes(16, 6), bytes(16, 7), new TargetStoreBackend.WriteLimits(64, 2 << 20));
        final var reader = new TargetQueueSnapshotReader(backend, 1);
        final TargetStoreBackend.ReadAuthority authority =
                (metadata, exactScope) -> new TargetStoreBackend.CommitGuard() {
                    @Override
                    public void requireCurrent() {
                        assertEquals(store.metadata(), metadata);
                        assertEquals(scope, exactScope);
                    }

                    @Override
                    public void close() {}
                };
        return new TargetWorkerTargetInventory.ShardSource() {
            @Override
            public ShardId shardId() {
                return store.shardId();
            }

            @Override
            public TargetQueueSnapshotReader.Page scan(
                    final BoundedReadBudget budget, final TargetPartitionId after, final int pageTargets) {
                return reader.scan(budget, after, pageTargets, authority);
            }

            @Override
            public TargetQueueSnapshotReader.Cut readCut(final BoundedReadBudget budget) {
                return reader.readCut(budget, authority);
            }
        };
    }

    private static TargetQueueSnapshotReader.Page page(
            final List<TargetQueueSnapshotReader.Entry> entries,
            final TargetQueueSnapshotReader.Stop stop,
            final TargetQueueSnapshotReader.Cut cut) {
        return new TargetQueueSnapshotReader.Page(
                entries, entries.isEmpty() ? null : entries.getLast().queue().targetId(), stop, cut);
    }

    private static TargetQueueSnapshotReader.Entry entry(final CanonicalTargetPartition physical) {
        return new TargetQueueSnapshotReader.Entry(
                new TargetQueueState(
                        physical.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, bytes(16, 3), 0, List.of()),
                physical);
    }

    private static CanonicalTargetPartition target(final int partition) {
        return new CanonicalTargetPartition(
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", new UUID(1, 2))), partition);
    }

    private static ShardId shard(final int partition) {
        return new ShardId(new RouteIncarnation(bytes(16, 4)), partition);
    }

    private static TargetQueueSnapshotReader.Cut cut(final int incarnation, final long sequence) {
        return new TargetQueueSnapshotReader.Cut(bytes(16, incarnation), sequence);
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static final class FakeSource implements TargetWorkerTargetInventory.ShardSource {
        private final ShardId shard;
        private final List<TargetQueueSnapshotReader.Page> pages;
        private final TargetQueueSnapshotReader.Cut endCut;
        private int cursor;

        private FakeSource(
                final ShardId shard,
                final List<TargetQueueSnapshotReader.Page> pages,
                final TargetQueueSnapshotReader.Cut endCut) {
            this.shard = shard;
            this.pages = pages;
            this.endCut = endCut;
        }

        @Override
        public ShardId shardId() {
            return shard;
        }

        @Override
        public TargetQueueSnapshotReader.Page scan(
                final BoundedReadBudget budget, final TargetPartitionId after, final int pageTargets) {
            if (cursor > 0) {
                assertEquals(pages.get(cursor - 1).nextAfter(), after);
            }
            assertEquals(1, pageTargets);
            return pages.get(cursor++);
        }

        @Override
        public TargetQueueSnapshotReader.Cut readCut(final BoundedReadBudget budget) {
            return endCut;
        }
    }
}

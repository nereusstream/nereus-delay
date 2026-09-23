package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetWorkerTargetInventoryTest {
    private static final TargetWorkerTargetInventory.Limits LIMITS =
            new TargetWorkerTargetInventory.Limits(2, 2, 1, 3, 10, 1 << 20, 1_000_000_000L);

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

package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetWorkerOrdinaryDrrTest {
    private static final TargetWorkerOrdinaryDrr.Limits ONE_VISIT =
            new TargetWorkerOrdinaryDrr.Limits(100, 200, 200, 1, 20, 1 << 20, 1_000_000_000L);
    private static final TargetWorkerOrdinaryDrr.Limits TWO_VISITS =
            new TargetWorkerOrdinaryDrr.Limits(100, 200, 200, 2, 20, 1 << 20, 1_000_000_000L);

    @Test
    void rejectsAnEnvelopeSmallerThanTheActivatedMaximumCost() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetWorkerOrdinaryDrr.Limits(100, 200, 199, 1, 20, 1 << 20, 1_000_000_000L));
    }

    @Test
    void givesEachPhysicalTargetOneByteShareAndWaitsForTheLargerHead() {
        final ShardId firstShard = shard(1);
        final ShardId secondShard = shard(2);
        final var physical = target(0);
        final var first = head(physical, firstShard, 200);
        final var second = head(physical, secondShard, 50);
        final var reads = new FakeReads(first, second);
        final var drr = schedule(List.of(targetState(physical, first, second)), reads, ONE_VISIT);
        final var oneVisit = new SchedulerBudget(1, 200, 1_000_000_000L);

        final var firstTurn = drr.runOrdinary(100, oneVisit, (shard, cost) -> Optional.of(() -> shard));
        assertTrue(firstTurn.claims().isEmpty());
        assertEquals(1, firstTurn.targetVisits());

        final var secondTurn = drr.runOrdinary(100, oneVisit, (shard, cost) -> Optional.of(() -> shard));
        assertEquals(List.of(firstShard), secondTurn.claims());
        assertEquals(200, secondTurn.schedulingBytes());

        final var thirdTurn = drr.runOrdinary(100, oneVisit, (shard, cost) -> Optional.of(() -> shard));
        assertEquals(List.of(secondShard), thirdTurn.claims());
        assertEquals(50, thirdTurn.schedulingBytes());
    }

    @Test
    void rotatesPhysicalTargetsAndSkipsAHeadThatCannotFitThisTurn() {
        final ShardId shard = shard(1);
        final var physicalA = target(0);
        final var physicalB = target(1);
        final var first = head(physicalA, shard, 200);
        final var second = head(physicalB, shard, 50);
        final var reads = new FakeReads(first, second);
        final var drr =
                schedule(List.of(targetState(physicalA, first), targetState(physicalB, second)), reads, TWO_VISITS);

        final var turn = drr.runOrdinary(
                100,
                new SchedulerBudget(2, 100, 1_000_000_000L),
                (source, cost) -> Optional.of(() -> cost.head().target()));
        assertEquals(List.of(physicalB.id()), turn.claims());
        assertEquals(2, turn.targetVisits());
        assertEquals(50, turn.schedulingBytes());
    }

    @Test
    void skipsOneBlockedSourceAndNeverAdvancesItsCursorOnFailedClaim() {
        final ShardId firstShard = shard(1);
        final ShardId secondShard = shard(2);
        final var physical = target(0);
        final var first = head(physical, firstShard, 50);
        final var second = head(physical, secondShard, 50);
        final var drr =
                schedule(List.of(targetState(physical, first, second)), new FakeReads(first, second), ONE_VISIT);
        final var oneVisit = new SchedulerBudget(1, 100, 1_000_000_000L);

        assertThrows(
                IllegalStateException.class,
                () -> drr.runOrdinary(
                        100,
                        oneVisit,
                        (shard, cost) -> Optional.of(() -> {
                            throw new IllegalStateException("Claim did not commit");
                        })));
        final var claimed = drr.runOrdinary(100, oneVisit, (shard, cost) -> Optional.of(() -> shard));
        assertEquals(List.of(firstShard), claimed.claims());
        final var next = drr.runOrdinary(100, oneVisit, (shard, cost) -> Optional.of(() -> shard));
        assertEquals(List.of(secondShard), next.claims());
    }

    @Test
    void staleHeadRefreshSkipsOnlyThatSourceAndStillServesAnother() {
        final ShardId firstShard = shard(1);
        final ShardId secondShard = shard(2);
        final var physical = target(0);
        final var first = head(physical, firstShard, 50);
        final var second = head(physical, secondShard, 50);
        final var reads = new FakeReads(first, second);
        reads.failOnce(first.ref());
        final var drr = schedule(List.of(targetState(physical, first, second)), reads, ONE_VISIT);

        final var turn = drr.runOrdinary(
                100, new SchedulerBudget(1, 100, 1_000_000_000L), (shard, cost) -> Optional.of(() -> shard));
        assertEquals(List.of(secondShard), turn.claims());
    }

    private static TargetWorkerOrdinaryDrr schedule(
            final List<TargetWorkerTargetInventory.Target> targets,
            final FakeReads reads,
            final TargetWorkerOrdinaryDrr.Limits limits) {
        return new TargetWorkerOrdinaryDrr(
                new TargetWorkerTargetInventory.Snapshot(targets, reads.cuts()), limits, reads, () -> 0);
    }

    private static TargetWorkerTargetInventory.Target targetState(
            final CanonicalTargetPartition physical, final Head... heads) {
        return new TargetWorkerTargetInventory.Target(
                physical,
                java.util.Arrays.stream(heads)
                        .map(head -> new TargetWorkerTargetInventory.Source(head.shard(), head.entry()))
                        .toList());
    }

    private static Head head(final CanonicalTargetPartition physical, final ShardId shard, final long cost) {
        final var domain = new TargetKeyCodec.Domain(0, 1);
        final var message = DelayMessageId.random(shard);
        final byte[] token = Bytes.concat(new byte[] {1}, Bytes.u64be(1));
        final var ref = new TargetHeadRef(
                TargetKeyCodec.candidate(
                        TargetKeyCodec.CandidateKind.DUE, physical.id(), domain, 10, token, message, 1),
                message,
                1,
                10);
        final var summary = new TargetDomainState(
                domain, TargetDomainState.Lifecycle.ACTIVE, bytes(32, 3), bytes(32, 4), null, ref, null);
        final var queue = new TargetQueueState(
                physical.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, bytes(16, 5), 0, List.of(summary));
        return new Head(shard, new TargetQueueSnapshotReader.Entry(queue, physical), ref, cost);
    }

    private static CanonicalTargetPartition target(final int partition) {
        return new CanonicalTargetPartition(
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", new UUID(1, 2))), partition);
    }

    private static ShardId shard(final int partition) {
        return new ShardId(new RouteIncarnation(bytes(16, 6)), partition);
    }

    private static byte[] bytes(final int size, final int value) {
        final byte[] result = new byte[size];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }

    private record Head(ShardId shard, TargetQueueSnapshotReader.Entry entry, TargetHeadRef ref, long cost) {}

    private record Key(ShardId shard, TargetPartitionId target) {}

    private static final class FakeReads implements TargetWorkerOrdinaryDrr.Reads {
        private static final TargetQuotaAccounting ACCOUNTING = new TargetQuotaAccounting(bytes(32, 7), 0, 0, 0, 1);
        private final Map<Key, TargetQueueSnapshotReader.Entry> entries = new HashMap<>();
        private final Map<TargetHeadRef, Long> costs = new HashMap<>();
        private final Map<ShardId, TargetQueueSnapshotReader.Cut> cuts = new HashMap<>();
        private final Set<TargetHeadRef> failOnce = new HashSet<>();

        private FakeReads(final Head... heads) {
            for (Head head : heads) {
                entries.put(new Key(head.shard(), head.ref().target()), head.entry());
                costs.put(head.ref(), head.cost());
                cuts.put(head.shard(), new TargetQueueSnapshotReader.Cut(bytes(16, 8), 1));
            }
        }

        private Map<ShardId, TargetQueueSnapshotReader.Cut> cuts() {
            return Map.copyOf(cuts);
        }

        private void failOnce(final TargetHeadRef head) {
            failOnce.add(head);
        }

        @Override
        public boolean membershipCurrent() {
            return true;
        }

        @Override
        public Optional<TargetQueueSnapshotReader.Entry> refresh(final ShardId shard, final TargetPartitionId target) {
            return Optional.ofNullable(entries.get(new Key(shard, target)));
        }

        @Override
        public TargetHeadCostProbe.Cost probe(final ShardId shard, final TargetHeadRef head) {
            final var key = new Key(shard, head.target());
            final var entry = entries.get(key);
            if (failOnce.remove(head)) {
                final var queue = entry.queue();
                final var prior = queue.domains().getFirst();
                final var emptied = new TargetDomainState(
                        prior.domain(),
                        prior.lifecycle(),
                        prior.dispatchCompatibilityRef(),
                        prior.controlScopeRef(),
                        prior.nativePolicyScopeRef(),
                        null,
                        null);
                entries.put(
                        key,
                        new TargetQueueSnapshotReader.Entry(
                                new TargetQueueState(
                                        queue.targetId(),
                                        queue.headRevision() + 1,
                                        queue.controlVersion(),
                                        queue.admissionState(),
                                        queue.accountingIncarnation(),
                                        queue.nativeIndexLeadCapMs(),
                                        List.of(emptied)),
                                entry.physical()));
                throw new IllegalStateException("selected head changed");
            }
            final long cost = costs.get(head);
            return new TargetHeadCostProbe.Cost(head, entry.queue(), ACCOUNTING, cost, cost, 10, 1_000);
        }
    }
}

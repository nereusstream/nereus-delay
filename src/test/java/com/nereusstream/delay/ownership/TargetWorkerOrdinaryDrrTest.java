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
import com.nereusstream.delay.store.BoundedReadBudget;
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
    void continuouslyEligibleTargetsAndSourceShardsMeetTheVisitBound() {
        final ShardId firstShard = shard(1);
        final ShardId secondShard = shard(2);
        final var shared = target(0);
        final var small = target(1);
        final var medium = target(2);
        final var sharedFirst = head(shared, firstShard, 200);
        final var sharedSecond = head(shared, secondShard, 200);
        final var smallHead = head(small, firstShard, 50);
        final var mediumHead = head(medium, secondShard, 150);
        final var drr = schedule(
                List.of(
                        targetState(shared, sharedFirst, sharedSecond),
                        targetState(small, smallHead),
                        targetState(medium, mediumHead)),
                new FakeReads(sharedFirst, sharedSecond, smallHead, mediumHead),
                ONE_VISIT);
        final var budget = new SchedulerBudget(1, 200, 1_000_000_000L);
        final Map<TargetPartitionId, Integer> lastTargetClaim = new HashMap<>();
        final Map<ShardId, Integer> lastSharedSourceClaim = new HashMap<>();
        final int targetBound = 3 * 2; // N * ceil(Cmax / Q)
        final int sharedSourceBound = 2 * targetBound; // S * N * ceil(Cmax / Q)

        for (int visit = 1; visit <= 48; visit++) {
            final TargetWorkerOrdinaryDrr.Turn<Service> turn = drr.runOrdinary(
                    100,
                    budget,
                    (source, cost) -> Optional.of(() -> new Service(cost.head().target(), source)));
            assertEquals(1, turn.targetVisits());
            for (Service service : turn.claims()) {
                assertTrue(visit - lastTargetClaim.getOrDefault(service.target(), 0) <= targetBound);
                lastTargetClaim.put(service.target(), visit);
                if (service.target().equals(shared.id())) {
                    assertTrue(visit - lastSharedSourceClaim.getOrDefault(service.source(), 0) <= sharedSourceBound);
                    lastSharedSourceClaim.put(service.source(), visit);
                }
            }
        }

        assertEquals(Set.of(shared.id(), small.id(), medium.id()), lastTargetClaim.keySet());
        assertEquals(Set.of(firstShard, secondShard), lastSharedSourceClaim.keySet());
        for (int last : lastTargetClaim.values()) {
            assertTrue(48 - last < targetBound);
        }
        for (int last : lastSharedSourceClaim.values()) {
            assertTrue(48 - last < sharedSourceBound);
        }
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

    @Test
    void incompleteReadYieldsWithoutDroppingTheHeadOrBlockingAnotherTarget() {
        final ShardId shard = shard(1);
        final var physicalA = target(0);
        final var physicalB = target(1);
        final var first = head(physicalA, shard, 50);
        final var second = head(physicalB, shard, 50);
        final var reads = new FakeReads(first, second);
        reads.incompleteOnce(shard, physicalA.id());
        final var drr =
                schedule(List.of(targetState(physicalA, first), targetState(physicalB, second)), reads, ONE_VISIT);
        final var budget = new SchedulerBudget(1, 100, 1_000_000_000L);

        final var yielded = drr.runOrdinary(
                100, budget, (source, cost) -> Optional.of(() -> cost.head().target()));
        assertEquals(TargetWorkerOrdinaryDrr.Stop.READ_INCOMPLETE, yielded.stop());
        assertTrue(yielded.claims().isEmpty());
        assertEquals(1, yielded.targetVisits());
        assertEquals(
                List.of(physicalB.id()),
                drr.runOrdinary(
                                100,
                                budget,
                                (source, cost) -> Optional.of(() -> cost.head().target()))
                        .claims());
        assertEquals(
                List.of(physicalA.id()),
                drr.runOrdinary(
                                100,
                                budget,
                                (source, cost) -> Optional.of(() -> cost.head().target()))
                        .claims());
    }

    @Test
    void recoveryFirstPassWaitsForTheLargeTargetBeforeRepeatingAClaimedTarget() {
        final ShardId shard = shard(1);
        final var physicalA = target(0);
        final var physicalB = target(1);
        final var first = head(physicalA, shard, 50);
        final var second = head(physicalB, shard, 200);
        final var reads = new FakeReads(first, second);
        final var drr = recoverySchedule(
                List.of(targetState(physicalA, first), targetState(physicalB, second)), reads, TWO_VISITS);
        final var budget = new SchedulerBudget(2, 200, 1_000_000_000L);
        final var commits = new java.util.concurrent.atomic.AtomicInteger();
        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> selector = (source, cost) -> Optional.of(() -> {
            commits.incrementAndGet();
            return cost.head().target();
        });

        assertThrows(IllegalStateException.class, () -> drr.runOrdinary(100, budget, selector));
        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.READY,
                drr.freezeFirstPass(100, budget, selector).stop());
        assertEquals(0, commits.get());
        assertEquals(
                List.of(physicalA.id()), drr.runOrdinary(100, budget, selector).claims());
        final var creditWait = drr.runOrdinary(100, budget, selector);
        assertTrue(creditWait.claims().isEmpty());
        assertEquals(2, creditWait.targetVisits());
        assertEquals(
                List.of(physicalB.id()), drr.runOrdinary(100, budget, selector).claims());
        assertEquals(
                List.of(physicalA.id()), drr.runOrdinary(100, budget, selector).claims());
        assertEquals(3, commits.get());
    }

    @Test
    void recoveryFirstPassRemovesBlockedTargetAndYieldsOnIncompleteFreezeRead() {
        final ShardId shard = shard(1);
        final var physicalA = target(0);
        final var physicalB = target(1);
        final var first = head(physicalA, shard, 50);
        final var second = head(physicalB, shard, 50);
        final var reads = new FakeReads(first, second);
        reads.incompleteOnce(shard, physicalA.id());
        final var drr = recoverySchedule(
                List.of(targetState(physicalA, first), targetState(physicalB, second)), reads, ONE_VISIT);
        final var budget = new SchedulerBudget(1, 200, 1_000_000_000L);
        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> eligible =
                (source, cost) -> Optional.of(() -> cost.head().target());

        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.READ_INCOMPLETE,
                drr.freezeFirstPass(100, budget, eligible).stop());
        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.VISIT_BUDGET,
                drr.freezeFirstPass(100, budget, eligible).stop());
        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.READY,
                drr.freezeFirstPass(100, budget, eligible).stop());
        assertEquals(
                List.of(physicalA.id()), drr.runOrdinary(100, budget, eligible).claims());
        final var blocked = drr.runOrdinary(
                100,
                budget,
                (source, cost) -> cost.head().target().equals(physicalB.id())
                        ? Optional.empty()
                        : Optional.of(() -> cost.head().target()));
        assertTrue(blocked.claims().isEmpty());
        assertEquals(
                List.of(physicalA.id()), drr.runOrdinary(100, budget, eligible).claims());
    }

    @Test
    void recoveryFirstPassRejectsAStoreCutChangedDuringTheFreeze() {
        final ShardId shard = shard(1);
        final var physicalA = target(0);
        final var physicalB = target(1);
        final var first = head(physicalA, shard, 50);
        final var second = head(physicalB, shard, 50);
        final var reads = new FakeReads(first, second);
        final var drr = recoverySchedule(
                List.of(targetState(physicalA, first), targetState(physicalB, second)), reads, ONE_VISIT);
        final var budget = new SchedulerBudget(1, 200, 1_000_000_000L);
        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> selector =
                (source, cost) -> Optional.of(() -> cost.head().target());

        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.VISIT_BUDGET,
                drr.freezeFirstPass(100, budget, selector).stop());
        reads.advanceCut(shard);
        assertThrows(IllegalStateException.class, () -> drr.freezeFirstPass(100, budget, selector));
        assertThrows(IllegalStateException.class, () -> drr.runOrdinary(100, budget, selector));
    }

    @Test
    void newlyAvailableTargetDoesNotReopenTheFrozenFirstPass() {
        final ShardId shard = shard(1);
        final var initiallyBlocked = target(0);
        final var eligible = target(1);
        final var blockedHead = head(initiallyBlocked, shard, 50);
        final var eligibleHead = head(eligible, shard, 50);
        final var reads = new FakeReads(blockedHead, eligibleHead);
        final var drr = recoverySchedule(
                List.of(targetState(initiallyBlocked, blockedHead), targetState(eligible, eligibleHead)),
                reads,
                TWO_VISITS);
        final var budget = new SchedulerBudget(2, 100, 1_000_000_000L);
        final var frozen = drr.freezeFirstPass(
                100,
                budget,
                (source, cost) -> cost.head().target().equals(initiallyBlocked.id())
                        ? Optional.empty()
                        : Optional.of(() -> cost.head().target()));
        assertEquals(TargetWorkerOrdinaryDrr.FreezeStop.READY, frozen.stop());
        assertEquals(1, frozen.eligibleTargets());

        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> available =
                (source, cost) -> Optional.of(() -> cost.head().target());
        assertEquals(
                List.of(eligible.id()), drr.runOrdinary(100, budget, available).claims());
        assertEquals(
                List.of(initiallyBlocked.id()),
                drr.runOrdinary(100, budget, available).claims());
    }

    @Test
    void blockedHeadIsRemovedEvenWhenThisTurnsBytesCannotFitIt() {
        final ShardId shard = shard(1);
        final var blocked = target(0);
        final var available = target(1);
        final var first = head(blocked, shard, 200);
        final var second = head(available, shard, 50);
        final var reads = new FakeReads(first, second);
        final var drr = recoverySchedule(
                List.of(targetState(blocked, first), targetState(available, second)), reads, TWO_VISITS);
        final var freezeBudget = new SchedulerBudget(2, 200, 1_000_000_000L);
        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> all =
                (source, cost) -> Optional.of(() -> cost.head().target());
        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.READY,
                drr.freezeFirstPass(100, freezeBudget, all).stop());

        final var smallBudget = new SchedulerBudget(2, 100, 1_000_000_000L);
        final var firstTurn = drr.runOrdinary(
                100,
                smallBudget,
                (source, cost) -> cost.head().target().equals(blocked.id())
                        ? Optional.empty()
                        : Optional.of(() -> cost.head().target()));
        assertEquals(List.of(available.id()), firstTurn.claims());
        assertEquals(
                List.of(available.id()), drr.runOrdinary(100, smallBudget, all).claims());
    }

    @Test
    void recoveryFreezeDoesNotPublishAfterItsElapsedBudget() {
        final ShardId shard = shard(1);
        final var physical = target(0);
        final var head = head(physical, shard, 50);
        final var reads = new FakeReads(head);
        final var ticks = new java.util.concurrent.atomic.AtomicLong();
        final var drr = new TargetWorkerOrdinaryDrr(
                new TargetWorkerTargetInventory.Snapshot(List.of(targetState(physical, head)), reads.cuts()),
                ONE_VISIT,
                reads,
                ticks::getAndIncrement,
                true);
        final var budget = new SchedulerBudget(1, 200, 3);
        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> selector =
                (source, cost) -> Optional.of(() -> cost.head().target());

        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.VISIT_BUDGET,
                drr.freezeFirstPass(100, budget, selector).stop());
        assertEquals(
                TargetWorkerOrdinaryDrr.FreezeStop.READY,
                drr.freezeFirstPass(100, budget, selector).stop());
    }

    @Test
    void futureTargetDoesNotHoldTheRecoveryFirstPassOpen() {
        final ShardId shard = shard(1);
        final var physical = target(0);
        final var head = head(physical, shard, 50);
        final var reads = new FakeReads(head);
        final var drr = recoverySchedule(List.of(targetState(physical, head)), reads, ONE_VISIT);
        final var budget = new SchedulerBudget(1, 100, 1_000_000_000L);
        final TargetWorkerOrdinaryDrr.Selector<TargetPartitionId> selector =
                (source, cost) -> Optional.of(() -> cost.head().target());

        final var frozen = drr.freezeFirstPass(5, budget, selector);
        assertEquals(TargetWorkerOrdinaryDrr.FreezeStop.READY, frozen.stop());
        assertEquals(0, frozen.eligibleTargets());
        assertTrue(drr.runOrdinary(5, budget, selector).claims().isEmpty());
        assertEquals(
                List.of(physical.id()), drr.runOrdinary(100, budget, selector).claims());
    }

    private static TargetWorkerOrdinaryDrr schedule(
            final List<TargetWorkerTargetInventory.Target> targets,
            final FakeReads reads,
            final TargetWorkerOrdinaryDrr.Limits limits) {
        return new TargetWorkerOrdinaryDrr(
                new TargetWorkerTargetInventory.Snapshot(targets, reads.cuts()), limits, reads, () -> 0);
    }

    private static TargetWorkerOrdinaryDrr recoverySchedule(
            final List<TargetWorkerTargetInventory.Target> targets,
            final FakeReads reads,
            final TargetWorkerOrdinaryDrr.Limits limits) {
        return new TargetWorkerOrdinaryDrr(
                new TargetWorkerTargetInventory.Snapshot(targets, reads.cuts()), limits, reads, () -> 0, true);
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

    private record Service(TargetPartitionId target, ShardId source) {}

    private record Key(ShardId shard, TargetPartitionId target) {}

    private static final class FakeReads implements TargetWorkerOrdinaryDrr.Reads {
        private static final TargetQuotaAccounting ACCOUNTING = new TargetQuotaAccounting(bytes(32, 7), 0, 0, 0, 1);
        private final Map<Key, TargetQueueSnapshotReader.Entry> entries = new HashMap<>();
        private final Map<TargetHeadRef, Long> costs = new HashMap<>();
        private final Map<ShardId, TargetQueueSnapshotReader.Cut> cuts = new HashMap<>();
        private final Set<TargetHeadRef> failOnce = new HashSet<>();
        private final Set<Key> incompleteOnce = new HashSet<>();

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

        private void incompleteOnce(final ShardId shard, final TargetPartitionId target) {
            incompleteOnce.add(new Key(shard, target));
        }

        private void advanceCut(final ShardId shard) {
            final var prior = cuts.get(shard);
            cuts.put(shard, new TargetQueueSnapshotReader.Cut(prior.storeIncarnation(), prior.nativeSequence() + 1));
        }

        @Override
        public boolean membershipCurrent() {
            return true;
        }

        @Override
        public boolean cutsCurrent(final Map<ShardId, TargetQueueSnapshotReader.Cut> expected) {
            return cuts.equals(expected);
        }

        @Override
        public Optional<TargetQueueSnapshotReader.Entry> refresh(final ShardId shard, final TargetPartitionId target) {
            final var key = new Key(shard, target);
            if (incompleteOnce.remove(key)) {
                final var budget = new BoundedReadBudget(1, 100, 1_000_000_000L, () -> 0);
                budget.tryCharge(1, 1);
                budget.beforeRead();
                throw budget.incomplete();
            }
            return Optional.ofNullable(entries.get(key));
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

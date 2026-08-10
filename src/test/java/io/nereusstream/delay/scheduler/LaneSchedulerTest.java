package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.SchedulerProjectionsV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.GenerationAggregateState;
import io.nereusstream.delay.runtime.GenerationRuntimeIndex;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.ReadyIndexValue;
import io.nereusstream.delay.runtime.TimelineEntry;
import io.nereusstream.delay.runtime.TimelineWorkRef;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneSchedulerTest {
    @TempDir
    Path tempDir;

    @Test
    void blockedLaneDoesNotPauseHealthyLane() {
        final DestinationLaneId bad = lane(1);
        final DestinationLaneId healthy = lane(2);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        scheduler.register(record(bad, 1));
        scheduler.register(record(healthy, 1));
        scheduler.offer(item(bad, 1));
        scheduler.offer(item(healthy, 2));
        scheduler.markBlocked(bad);

        final List<ScheduleWorkItem> result = scheduler.poll(new SchedulerBudget(8, 1024 * 1024, 1_000_000_000));

        assertEquals(List.of(healthy), result.stream().map(ScheduleWorkItem::laneId).toList());
        assertEquals(1, scheduler.pendingItems(bad));
        assertEquals(0, scheduler.pendingItems(healthy));
    }

    @Test
    void duePollUsesAnInclusiveEligibilityBoundary() {
        final DestinationLaneId lane = lane(2);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        scheduler.register(record(lane, 1));
        scheduler.offer(new ScheduleWorkItem(lane, DelayMessageId.random(
                new ShardId(RouteIncarnation.random(), 2)), 0, 2_000, 1));

        assertEquals(List.of(), scheduler.poll(1_999,
                new SchedulerBudget(1, 1024, 1_000_000_000)));
        assertEquals(List.of(lane), scheduler.poll(2_000,
                new SchedulerBudget(1, 1024, 1_000_000_000)).stream()
                .map(ScheduleWorkItem::laneId).toList());
    }

    @Test
    void queueSnapshotRestoresExactFifoProjection() {
        final DestinationLaneId lane = lane(2);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        scheduler.register(record(lane, 1));
        final ScheduleWorkItem first = item(lane, 1);
        final ScheduleWorkItem second = item(lane, 2);
        scheduler.offer(first);
        scheduler.offer(second);
        final var before = scheduler.queueSnapshot();

        scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000));
        scheduler.restoreQueues(before);

        assertEquals(before, scheduler.queueSnapshot());
        assertEquals(List.of(first), scheduler.poll(new SchedulerBudget(1, 1, 1_000_000_000)));
    }

    @Test
    void failedPendingReplacementKeepsTheOriginalQueues() {
        final DestinationLaneId healthy = lane(26);
        final DestinationLaneId paused = lane(27);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        scheduler.register(record(healthy, 1));
        scheduler.register(record(paused, 1).pauseByAdmin());
        scheduler.offer(item(healthy, 1));
        final var before = scheduler.queueSnapshot();

        assertThrows(IllegalStateException.class,
                () -> scheduler.replacePending(List.of(item(healthy, 2), item(paused, 2))));
        assertEquals(before, scheduler.queueSnapshot());
    }

    @Test
    void weightedDeficitRoundRobinEventuallyServicesBothLanes() {
        final DestinationLaneId first = lane(3);
        final DestinationLaneId second = lane(4);
        final LaneScheduler scheduler = new LaneScheduler(10, 64);
        scheduler.register(record(first, 2));
        scheduler.register(record(second, 1));
        for (int index = 0; index < 12; index++) {
            scheduler.offer(item(first, index));
            scheduler.offer(item(second, index));
        }

        final List<ScheduleWorkItem> firstVisit = scheduler.poll(new SchedulerBudget(64, 100, 1_000_000_000));
        final List<ScheduleWorkItem> secondVisit = scheduler.poll(new SchedulerBudget(64, 100, 1_000_000_000));

        // Both visits make progress and the lower-weight lane is not starved.
        org.junit.jupiter.api.Assertions.assertFalse(firstVisit.isEmpty());
        org.junit.jupiter.api.Assertions.assertFalse(secondVisit.isEmpty());
        org.junit.jupiter.api.Assertions.assertTrue(firstVisit.stream().anyMatch(item -> item.laneId().equals(second))
                || secondVisit.stream().anyMatch(item -> item.laneId().equals(second)));
    }

    @Test
    void highWeightRetainsItsConfiguredDeficitQuantum() {
        final DestinationLaneId lane = lane(24);
        final LaneScheduler scheduler = new LaneScheduler(10, 1);
        scheduler.register(record(lane, 8));
        scheduler.offer(item(lane, 1));

        scheduler.poll(new SchedulerBudget(1, 100, 1_000_000_000));

        assertEquals(79, scheduler.snapshot().lanes().get(0).deficit());
    }

    @Test
    void weightDowngradeRecomputesDeficitCapAndClampsExistingCredit() {
        final DestinationLaneId lane = lane(52);
        final LaneScheduler scheduler = new LaneScheduler(10, 1);
        scheduler.register(record(lane, 8));
        scheduler.offer(item(lane, 1));
        scheduler.poll(new SchedulerBudget(1, 100, 1_000_000_000));
        assertEquals(79, scheduler.snapshot().lanes().get(0).deficit());

        scheduler.register(record(lane, 1));

        assertEquals(40, scheduler.snapshot().lanes().get(0).deficit());
    }

    @Test
    void rejectsQuantumAndWeightArithmeticOverflow() {
        assertThrows(IllegalArgumentException.class,
                () -> new LaneScheduler(Long.MAX_VALUE, 1));

        final LaneScheduler scheduler = new LaneScheduler(Long.MAX_VALUE / 4, 1);
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.register(record(lane(20), Integer.MAX_VALUE)));
    }

    @Test
    void rejectsLaneIncarnationChangeWithoutMutatingSchedulerState() {
        final DestinationLaneId lane = lane(25);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        scheduler.register(recordWithIncarnation(lane, 1));
        scheduler.offer(item(lane, 1));
        final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

        assertThrows(IllegalArgumentException.class,
                () -> scheduler.register(recordWithIncarnation(lane, 2)));

        assertEquals(before, scheduler.snapshot());
        assertEquals(1, scheduler.pendingItems(lane));
    }

    @Test
    void terminalLaneUnregisterRequiresExactIncarnationAndEmptyQueue() {
        final DestinationLaneId lane = lane(34);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        final LaneRecord active = recordWithIncarnation(lane, 1);
        scheduler.register(active);
        scheduler.offer(item(lane, 1));
        final LaneRecord closed = active.closeForNewAdmission();
        scheduler.register(closed);

        assertThrows(IllegalStateException.class,
                () -> scheduler.unregister(lane, closed.laneIncarnation()));
        scheduler.replacePending(List.of());
        final byte[] staleIncarnation = closed.laneIncarnation();
        staleIncarnation[0] = 2;
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.unregister(lane, staleIncarnation));

        scheduler.unregister(lane, closed.laneIncarnation());
        assertEquals(List.of(), scheduler.snapshot().lanes());
        assertEquals(List.of(), scheduler.ringOrder());
        assertThrows(IllegalArgumentException.class, () -> scheduler.pendingItems(lane));
    }

    @Test
    void ringVisitLimitUsesWideArithmetic() {
        assertEquals(0, LaneScheduler.boundedRingVisitLimit(0));
        assertEquals(4_294_967_292L,
                LaneScheduler.boundedRingVisitLimit(Integer.MAX_VALUE - 1));
        assertEquals(4_294_967_294L,
                LaneScheduler.boundedRingVisitLimit(Integer.MAX_VALUE));
    }

    @Test
    void saturatesRestoredDeficitBeforeServing() {
        final DestinationLaneId lane = lane(21);
        final LaneScheduler scheduler = new LaneScheduler(10, 1);
        scheduler.register(record(lane, 1));
        scheduler.offer(item(lane, 1));
        scheduler.restore(new LaneScheduler.SchedulerSnapshot(0, 0,
                List.of(new LaneScheduler.LaneSnapshot(lane, 1, Long.MAX_VALUE, 0, 1, true))));

        assertEquals(40, scheduler.snapshot().lanes().get(0).deficit());
        assertEquals(List.of(lane), scheduler.poll(new SchedulerBudget(1, 100, 1_000_000_000)).stream()
                .map(ScheduleWorkItem::laneId).toList());
        assertEquals(39, scheduler.snapshot().lanes().get(0).deficit());
    }

    @Test
    void invalidLaterRestoreEntryDoesNotPartiallyApplyEarlierCounters() {
        final DestinationLaneId first = lane(23);
        final DestinationLaneId second = lane(24);
        final LaneScheduler scheduler = new LaneScheduler(10, 2);
        scheduler.register(record(first, 1));
        scheduler.register(record(second, 1));
        final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

        assertThrows(IllegalArgumentException.class, () -> scheduler.restore(
                new LaneScheduler.SchedulerSnapshot(0, 0, List.of(
                        new LaneScheduler.LaneSnapshot(first, 1, Long.MAX_VALUE, 7, 0, true),
                        new LaneScheduler.LaneSnapshot(second, 1, -1, 0, 0, true)))));

        assertEquals(before, scheduler.snapshot());
    }

    @Test
    void duplicateLaneRestoreIdentityDoesNotPartiallyApplyEarlierCounters() {
        final DestinationLaneId first = lane(25);
        final LaneScheduler scheduler = new LaneScheduler(10, 2);
        scheduler.register(record(first, 1));
        final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

        assertThrows(IllegalArgumentException.class, () -> scheduler.restore(
                new LaneScheduler.SchedulerSnapshot(0, 0, List.of(
                        new LaneScheduler.LaneSnapshot(first, 1, 20, 3, 0, true),
                        new LaneScheduler.LaneSnapshot(first, 1, 0, 0, 0, true)))));
        assertEquals(before, scheduler.snapshot());
    }

    @Test
    void saturatesRoundGenerationBeforeServingAtLongMaximum() {
        final DestinationLaneId lane = lane(22);
        final LaneScheduler scheduler = new LaneScheduler(10, 1);
        scheduler.register(record(lane, 1));
        scheduler.offer(item(lane, 1));
        scheduler.restore(new LaneScheduler.SchedulerSnapshot(0, Long.MAX_VALUE,
                List.of(new LaneScheduler.LaneSnapshot(lane, 1, 10, Long.MAX_VALUE, 1, true))));

        assertEquals(List.of(lane), scheduler.poll(new SchedulerBudget(1, 100, 1_000_000_000)).stream()
                .map(ScheduleWorkItem::laneId).toList());
        assertEquals(Long.MAX_VALUE, scheduler.snapshot().roundGeneration());
        assertEquals(Long.MAX_VALUE, scheduler.snapshot().lanes().get(0).lastServedRound());
    }

    @Test
    void fairnessCountersSurviveOwnerRestart() {
        final DestinationLaneId lane = lane(5);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 5);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        LaneScheduler.SchedulerSnapshot saved;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(lane, 2));
            scheduler.offer(item(lane, 1));
            scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000));
            saved = scheduler.snapshot();
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(lane, 2));
            scheduler.restorePersistedState();
            final LaneScheduler.LaneSnapshot restored = scheduler.snapshot().lanes().get(0);
            final LaneScheduler.LaneSnapshot expected = saved.lanes().get(0);
            assertEquals(expected.deficit(), restored.deficit());
            assertEquals(expected.lastServedRound(), restored.lastServedRound());
            assertEquals(expected.weight(), restored.weight());
        }
    }

    @Test
    void fairnessCountersSurviveRestartForLaneOutsideActiveRing() {
        final DestinationLaneId first = lane(26);
        final DestinationLaneId blocked = lane(27);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 26);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("blocked-lane-counters"));
        LaneScheduler.LaneSnapshot expected;

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(first, 1));
            scheduler.register(record(blocked, 1));
            scheduler.offer(item(first, 1));
            scheduler.offer(item(blocked, 1));
            scheduler.poll(new SchedulerBudget(2, 1024, 1_000_000_000));
            scheduler.markBlocked(blocked);
            expected = scheduler.snapshot().lanes().stream()
                    .filter(state -> state.laneId().equals(blocked))
                    .findFirst().orElseThrow();
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(first, 1));
            scheduler.register(record(blocked, 1));
            scheduler.restorePersistedState();
            final LaneScheduler.LaneSnapshot restored = scheduler.snapshot().lanes().stream()
                    .filter(state -> state.laneId().equals(blocked))
                    .findFirst().orElseThrow();
            assertEquals(expected.deficit(), restored.deficit());
            assertEquals(expected.lastServedRound(), restored.lastServedRound());
        }
    }

    @Test
    void ownerChangeRestartsRecoveryFirstPassWithoutServingLaneTwice() {
        final DestinationLaneId first = lane(18);
        final DestinationLaneId second = lane(19);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("owner-change-first-pass"));
        final OwnerIdentityV1 firstOwner = owner(1);
        final OwnerIdentityV1 secondOwner = owner(2);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = new PersistentLaneScheduler(store, LaneScheduler.defaults(),
                    firstOwner);
            scheduler.register(record(first, 1));
            scheduler.register(record(second, 1));
            scheduler.offer(item(first, 1));
            scheduler.offer(item(first, 2));
            scheduler.offer(item(second, 1));
            scheduler.offer(item(second, 2));

            final List<ScheduleWorkItem> firstVisit = scheduler.poll(
                    new SchedulerBudget(8, 1024, 1_000_000_000));
            assertEquals(2, firstVisit.size());
            assertEquals(2, firstVisit.stream().map(ScheduleWorkItem::laneId).distinct().count());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = new PersistentLaneScheduler(store, LaneScheduler.defaults(),
                    secondOwner);
            scheduler.register(record(first, 1));
            scheduler.register(record(second, 1));
            scheduler.restorePersistedState();
            scheduler.offer(item(first, 3));
            scheduler.offer(item(first, 4));
            scheduler.offer(item(second, 3));
            scheduler.offer(item(second, 4));

            final List<ScheduleWorkItem> firstVisitAfterOwnerChange = scheduler.poll(
                    new SchedulerBudget(8, 1024, 1_000_000_000));
            assertEquals(2, firstVisitAfterOwnerChange.size());
            assertEquals(2, firstVisitAfterOwnerChange.stream()
                    .map(ScheduleWorkItem::laneId).distinct().count());
        }
    }

    @Test
    void persistsAllFiveClosedSchedulerProjectionsTogether() {
        final DestinationLaneId lane = lane(6);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 6);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(lane, 1));
            scheduler.offer(item(lane, 1));
            scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000));

            org.junit.jupiter.api.Assertions.assertNotNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(1), 5));
            org.junit.jupiter.api.Assertions.assertNotNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(2), 5));
            org.junit.jupiter.api.Assertions.assertNotNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(3), 5));
            org.junit.jupiter.api.Assertions.assertNotNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(4), 5));
            org.junit.jupiter.api.Assertions.assertNotNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(5), 5));
            SchedulerProjectionsV1.ActiveRing.decode(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(2), 5).payload());
            SchedulerProjectionsV1.Round.decode(store.getValue(ColumnFamily.META,
                    KeyCodec.metaScheduler(4), 5).payload());
        }
    }

    @Test
    void persistentTerminalLaneUnregisterRemovesFairnessProjection() {
        final DestinationLaneId lane = lane(35);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-lane-unregister"));
        final LaneRecord closed = recordWithIncarnation(lane, 1).closeForNewAdmission();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(closed);
            scheduler.unregister(lane, closed.laneIncarnation());
            assertEquals(List.of(), scheduler.snapshot().lanes());
            assertEquals(List.of(), SchedulerProjectionsV1.ActiveRing.decode(store.getValue(
                    ColumnFamily.META, KeyCodec.metaScheduler(2), 5).payload()).entries());
            assertEquals(List.of(), SchedulerProjectionsV1.DeficitMap.decode(store.getValue(
                    ColumnFamily.META, KeyCodec.metaScheduler(3), 5).payload()).entries());
            assertEquals(List.of(), SchedulerProjectionsV1.LastServedMap.decode(store.getValue(
                    ColumnFamily.META, KeyCodec.metaScheduler(5), 5).payload()).entries());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler recovered = PersistentLaneScheduler.defaults(store);
            recovered.restorePersistedState();
            assertEquals(List.of(), recovered.snapshot().lanes());
        }
    }

    @Test
    void failedPersistentLaneUnregisterRestoresInMemoryRegistration() {
        final DestinationLaneId lane = lane(36);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 36);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-lane-unregister-failure"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            try {
                final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
                final LaneRecord closed = recordWithIncarnation(lane, 1).closeForNewAdmission();
                scheduler.register(closed);
                final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

                store.close();
                assertThrows(IllegalStateException.class,
                        () -> scheduler.unregister(lane, closed.laneIncarnation()));
                assertEquals(before, scheduler.snapshot());
            } finally {
                store.close();
            }
        }
    }

    @Test
    void failedPersistentUnregisterDoesNotReactivatePreviouslyInactiveLane() {
        final DestinationLaneId lane = lane(37);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 37);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("scheduler-lane-unregister-inactive-failure"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            try {
                final LaneScheduler delegate = LaneScheduler.defaults();
                final PersistentLaneScheduler scheduler = new PersistentLaneScheduler(store, delegate);
                final LaneRecord closed = recordWithIncarnation(lane, 1).closeForNewAdmission();
                scheduler.register(closed);
                scheduler.markBlocked(lane);
                assertEquals(List.of(), delegate.ringOrder());
                final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

                store.close();
                assertThrows(IllegalStateException.class,
                        () -> scheduler.unregister(lane, closed.laneIncarnation()));
                assertEquals(before, scheduler.snapshot());
                assertEquals(List.of(), delegate.ringOrder());
            } finally {
                store.close();
            }
        }
    }

    @Test
    void failedSchedulerProjectionWriteDoesNotAdvanceGenerationInMemory() {
        final DestinationLaneId lane = lane(30);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 30);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-write-failure"));
        final long persistedGeneration;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            try {
                final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
                scheduler.register(record(lane, 1));
                scheduler.persist();
                persistedGeneration = scheduler.discoveryCursor().activeRingGeneration();

                store.close();
                assertThrows(IllegalStateException.class, scheduler::persist);
                assertEquals(persistedGeneration, scheduler.discoveryCursor().activeRingGeneration());
            } finally {
                store.close();
            }
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SchedulerProjectionsV1.ActiveRing activeRing = SchedulerProjectionsV1.ActiveRing.decode(
                    store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(2), 5).payload());
            assertEquals(persistedGeneration, activeRing.ringGeneration());
        }
    }

    @Test
    void failedPollProjectionWriteRestoresThePolledHeadInMemory() {
        final DestinationLaneId lane = lane(37);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 37);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-poll-write-failure"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            try {
                final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
                scheduler.register(record(lane, 1));
                scheduler.offer(item(lane, 1));
                scheduler.persist();
                final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

                store.close();
                assertThrows(IllegalStateException.class,
                        () -> scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000)));
                assertEquals(before, scheduler.snapshot());
            } finally {
                store.close();
            }
        }
    }

    @Test
    void failedReadinessProjectionWriteRestoresThePreviousGateProjection() {
        final DestinationLaneId lane = lane(38);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 38);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-ready-write-failure"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            try {
                final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
                scheduler.register(record(lane, 1));
                scheduler.markBlocked(lane);
                final LaneScheduler.SchedulerSnapshot before = scheduler.snapshot();

                store.close();
                assertThrows(IllegalStateException.class, () -> scheduler.markReady(lane));
                assertEquals(before, scheduler.snapshot());
            } finally {
                store.close();
            }
        }
    }

    @Test
    void failedReadyProjectionDecodeDoesNotAdvanceWrapGenerationInMemory() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-wrap-failure"));
        final LaneRecord firstLane = record(lane(31), 1);
        final LaneRecord lastLane = record(lane(32), 1);
        final SourcePosition firstSource = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 0,
                null, 1_000);
        final ReadyFixture first = readyFixture(firstLane, DelayMessageId.random(shardId),
                firstSource,
                new MessageRecord(MessageStatus.SCHEDULED, 1, 1, 1_000, 9_000, firstLane.laneId(),
                        OrderingMode.BEST_EFFORT, new byte[]{1}, firstSource.canonicalBytes()));
        final SourcePosition lastSource = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 1,
                null, 1_001);
        final ReadyFixture last = readyFixture(lastLane, DelayMessageId.random(shardId),
                lastSource,
                new MessageRecord(MessageStatus.SCHEDULED, 1, 1, 1_000, 9_000, lastLane.laneId(),
                        OrderingMode.BEST_EFFORT, new byte[]{2}, lastSource.canonicalBytes()));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                putReady(batch, first);
                putReady(batch, last);
            });
            final PersistentLaneScheduler initial = PersistentLaneScheduler.defaults(store);
            final LaneRecord registeredOnly = record(lane(33), 1);
            initial.register(registeredOnly);
            initial.persist();
            final long ringGeneration = SchedulerProjectionsV1.ActiveRing.decode(
                    store.getValue(ColumnFamily.META, KeyCodec.metaScheduler(2), 5).payload())
                    .ringGeneration();
            store.write(batch -> batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(1),
                    new SchedulerProjectionsV1.ReadyDiscoveryCursor(last.readyKey(), 0, ringGeneration)
                            .canonicalBytes()));

            final PersistentLaneScheduler recovered = PersistentLaneScheduler.defaults(store);
            recovered.register(registeredOnly);
            assertThrows(IllegalStateException.class,
                    () -> recovered.discoverReady(new SchedulerBudget(1, 1024, 1_000_000_000)));
            assertEquals(0, recovered.discoveryCursor().wrapGeneration());
        }
    }

    @Test
    void schedulerRestoreRejectsCrossProjectionGenerationDrift() {
        final DestinationLaneId lane = lane(28);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("scheduler-generation-drift"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(lane, 1));
            scheduler.offer(item(lane, 1));
            scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000));

            final SchedulerProjectionsV1.ReadyDiscoveryCursor discovery =
                    SchedulerProjectionsV1.ReadyDiscoveryCursor.decode(store.getValue(ColumnFamily.META,
                            KeyCodec.metaScheduler(1), 5).payload());
            final SchedulerProjectionsV1.ReadyDiscoveryCursor drifted =
                    new SchedulerProjectionsV1.ReadyDiscoveryCursor(discovery.lastScannedReadyKey(),
                            discovery.wrapGeneration(), discovery.activeRingGeneration() + 1);
            store.write(batch -> batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(1),
                    drifted.canonicalBytes()));

            assertThrows(IllegalStateException.class,
                    () -> new PersistentLaneScheduler(store, LaneScheduler.defaults()));
        }
    }

    @Test
    void malformedPersistedSchedulerGenerationDoesNotPartiallyApplyTheActiveRing() {
        final DestinationLaneId lane = lane(48);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 48);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("scheduler-restore-partial-generation"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SchedulerProjectionsV1.ReadyDiscoveryCursor discovery =
                    new SchedulerProjectionsV1.ReadyDiscoveryCursor(null, 0, 1);
            final SchedulerProjectionsV1.ActiveRing activeRing =
                    new SchedulerProjectionsV1.ActiveRing(1, -1, 0, List.of());
            final SchedulerProjectionsV1.DeficitMap deficits =
                    new SchedulerProjectionsV1.DeficitMap(List.of());
            final SchedulerProjectionsV1.Round round =
                    new SchedulerProjectionsV1.Round(-1, owner(1), false);
            final SchedulerProjectionsV1.LastServedMap lastServed =
                    new SchedulerProjectionsV1.LastServedMap(List.of());
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(1), discovery.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(2), activeRing.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(3), deficits.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(4), round.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(5), lastServed.canonicalBytes());
            });

            final LaneScheduler delegate = LaneScheduler.defaults();
            final PersistentLaneScheduler recovered = new PersistentLaneScheduler(store, delegate);
            recovered.register(record(lane, 1));
            final LaneScheduler.SchedulerSnapshot before = delegate.snapshot();
            final List<DestinationLaneId> beforeRing = delegate.ringOrder();

            assertThrows(IllegalArgumentException.class, recovered::restorePersistedState);
            assertEquals(before, delegate.snapshot());
            assertEquals(beforeRing, delegate.ringOrder());
        }
    }

    @Test
    void stalePersistedDeficitVersionDoesNotRestoreCreditsToARevisedLane() {
        final DestinationLaneId lane = lane(49);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 49);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("scheduler-stale-deficit-version"));
        final byte[] incarnation = new byte[16];
        final SchedulerProjectionsV1.ReadyDiscoveryCursor discovery =
                new SchedulerProjectionsV1.ReadyDiscoveryCursor(null, 0, 1);
        final SchedulerProjectionsV1.ActiveRing activeRing =
                new SchedulerProjectionsV1.ActiveRing(1, 5, 0, List.of(
                        new SchedulerProjectionsV1.RingEntry(lane, incarnation, 1)));
        final SchedulerProjectionsV1.DeficitMap deficits = new SchedulerProjectionsV1.DeficitMap(List.of(
                new SchedulerProjectionsV1.DeficitEntry(lane, incarnation, 64, 1)));
        final SchedulerProjectionsV1.Round round = new SchedulerProjectionsV1.Round(5, owner(1), false);
        final SchedulerProjectionsV1.LastServedMap lastServed = new SchedulerProjectionsV1.LastServedMap(List.of(
                new SchedulerProjectionsV1.LastServedEntry(lane, incarnation, 4, 1)));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(1), discovery.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(2), activeRing.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(3), deficits.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(4), round.canonicalBytes());
                batch.putValue(ColumnFamily.META, 5, KeyCodec.metaScheduler(5), lastServed.canonicalBytes());
            });

            final LaneScheduler delegate = LaneScheduler.defaults();
            final PersistentLaneScheduler recovered = new PersistentLaneScheduler(store, delegate);
            // The same Lane incarnation has advanced its runtime version. The
            // old active ring and deficit entry are stale, while last-served
            // history remains useful for the incarnation-level service gap.
            recovered.register(new LaneRecord(lane, incarnation, 1, 2, AdmissionGate.OPEN,
                    RuntimeReadiness.READY, 1, 0));
            recovered.restorePersistedState();

            final LaneScheduler.LaneSnapshot restored = delegate.snapshot().lanes().get(0);
            assertEquals(0, restored.deficit());
            assertEquals(4, restored.lastServedRound());
            assertEquals(List.of(), delegate.ringOrder());
        }
    }

    @Test
    void fencedRecoveryRebuildsReadyQueueFromLaneAndMessageIndexes() {
        final DestinationLaneId lane = lane(7);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 4,
                null, 1_004);
        final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1, AdmissionGate.OPEN,
                RuntimeReadiness.READY, 2, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 3, 4, 1_000, 9_000, lane,
                OrderingMode.BEST_EFFORT, new byte[]{1, 2, 3}, source.canonicalBytes());
        final byte[] readyKey = KeyCodec.timelineReady(1_000, lane, laneRecord.laneVersion());
        final byte[] timelineKey = KeyCodec.timelineDue(lane, 1_000, source.sourceOrderToken(), messageId,
                message.generation());
        final ReadyIndexValue ready = new ReadyIndexValue(lane, 1_000, laneRecord.laneVersion(), messageId,
                message.generation(), Bytes.sha256(timelineKey));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(lane),
                        LaneRecordEnvelopeV1.active(laneRecord.encode()).canonicalBytes());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), message.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey,
                        new TimelineEntry(messageId, message.generation()).encode());
                batch.putValue(ColumnFamily.TIMELINE, 3, readyKey, ready.encode());
            });
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(laneRecord);

            org.junit.jupiter.api.Assertions.assertEquals(1, scheduler.rebuildFromAuthoritativeReady(1));
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            assertEquals(messageId, scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000)).get(0).messageId());
            final SchedulerProjectionsV1.ReadyDiscoveryCursor cursor = scheduler.discoveryCursor();
            org.junit.jupiter.api.Assertions.assertArrayEquals(readyKey, cursor.lastScannedReadyKey());
        }
    }

    @Test
    void fencedRecoveryAcceptsCanonicalTimelineWorkRefValue() {
        final DestinationLaneId lane = lane(8);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready-rich-timeline"));
        final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 4,
                null, 1_004);
        final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1, AdmissionGate.OPEN,
                RuntimeReadiness.READY, 2, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord base = new MessageRecord(MessageStatus.SCHEDULED, 3, 4, 1_000, 9_000, lane,
                OrderingMode.BEST_EFFORT, new byte[]{1, 2, 3}, source.canonicalBytes());
        final byte[] timelineKey = KeyCodec.timelineDue(lane, 1_000, source.sourceOrderToken(), messageId,
                base.generation());
        final MessageRecord message = base.withRuntimeIndex(GenerationRuntimeIndex.timeline(
                GenerationAggregateState.SCHEDULED, TimelineWorkRef.initial(timelineKey, 1_000, 4),
                List.of(), 0, 0, false, 4));
        final byte[] readyKey = KeyCodec.timelineReady(1_000, lane, laneRecord.laneVersion());
        final ReadyIndexValue ready = new ReadyIndexValue(lane, 1_000, laneRecord.laneVersion(), messageId,
                message.generation(), Bytes.sha256(timelineKey));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(lane),
                        LaneRecordEnvelopeV1.active(laneRecord.encode()).canonicalBytes());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), message.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey,
                        message.runtimeIndex().timeline().canonicalBytes());
                batch.putValue(ColumnFamily.TIMELINE, 3, readyKey, ready.encode());
            });
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(laneRecord);

            assertEquals(1, scheduler.rebuildFromAuthoritativeReady(1));
            assertEquals(messageId, scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000)).get(0).messageId());
        }
    }

    @Test
    void fencedRecoveryRejectsReadyMessageFromAnotherShard() {
        final DestinationLaneId lane = lane(23);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 23);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 24);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready-source-shard-mismatch"));
        final SourcePosition foreignSource = new KafkaSourcePosition(otherShardId, "cluster", UUID.randomUUID(), 4,
                null, 1_004);
        final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1, AdmissionGate.OPEN,
                RuntimeReadiness.READY, 2, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 3, 4, 1_000, 9_000, lane,
                OrderingMode.BEST_EFFORT, new byte[]{1, 2, 3}, foreignSource.canonicalBytes());
        final byte[] readyKey = KeyCodec.timelineReady(1_000, lane, laneRecord.laneVersion());
        final byte[] timelineKey = KeyCodec.timelineDue(lane, 1_000, foreignSource.sourceOrderToken(), messageId,
                message.generation());
        final ReadyIndexValue ready = new ReadyIndexValue(lane, 1_000, laneRecord.laneVersion(), messageId,
                message.generation(), Bytes.sha256(timelineKey));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(lane),
                        LaneRecordEnvelopeV1.active(laneRecord.encode()).canonicalBytes());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), message.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey,
                        new TimelineEntry(messageId, message.generation()).encode());
                batch.putValue(ColumnFamily.TIMELINE, 3, readyKey, ready.encode());
            });
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(laneRecord);

            assertThrows(IllegalStateException.class, () -> scheduler.rebuildFromAuthoritativeReady(1));
        }
    }

    @Test
    void fencedRecoveryUsesCompleteReadyPassDespitePersistedDiscoveryCursor() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 29);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("ready-recovery-complete-pass"));
        final List<LaneRecord> lanes = new ArrayList<>();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                for (int index = 0; index < 4; index++) {
                    final DestinationLaneId lane = lane(30 + index);
                    final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1,
                            AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 1_000);
                    final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                            index, null, 1_000 + index);
                    final DelayMessageId messageId = DelayMessageId.random(shardId);
                    final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                            1_000, 9_000, lane, OrderingMode.BEST_EFFORT, new byte[]{(byte) index},
                            source.canonicalBytes());
                    final byte[] readyKey = KeyCodec.timelineReady(1_000, lane, laneRecord.laneVersion());
                    final byte[] timelineKey = KeyCodec.timelineDue(lane, 1_000, source.sourceOrderToken(),
                            messageId, message.generation());
                    final ReadyIndexValue ready = new ReadyIndexValue(lane, 1_000, laneRecord.laneVersion(),
                            messageId, message.generation(), Bytes.sha256(timelineKey));
                    lanes.add(laneRecord);
                    batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(lane),
                            LaneRecordEnvelopeV1.active(laneRecord.encode()).canonicalBytes());
                    batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), message.encode());
                    batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey,
                            new TimelineEntry(messageId, message.generation()).encode());
                    batch.putValue(ColumnFamily.TIMELINE, 3, readyKey, ready.encode());
                }
            });

            final PersistentLaneScheduler initial = PersistentLaneScheduler.defaults(store);
            lanes.forEach(initial::register);
            assertEquals(4, initial.rebuildFromAuthoritativeReady(4));

            // A recovery cursor points at the last scanned entry.  A complete
            // pass must still detect all four authoritative READY entries;
            // it must not treat the cursor as an item that can be discarded
            // from the bounded scan.
            final PersistentLaneScheduler recovered = PersistentLaneScheduler.defaults(store);
            lanes.forEach(recovered::register);
            assertThrows(IllegalStateException.class, () -> recovered.rebuildFromAuthoritativeReady(3));
        }
    }

    @Test
    void rotatingReadyDiscoveryDoesNotReofferPolledHeadAndFindsSuccessorAfterWrap() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 30);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("ready-discovery-rotation"));
        final List<LaneRecord> lanes = new ArrayList<>();
        final List<ReadyFixture> fixtures = new ArrayList<>();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                for (int index = 0; index < 2; index++) {
                    final DestinationLaneId lane = lane(40 + index);
                    final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1,
                            AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 1_000);
                    final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                            index, null, 1_000 + index);
                    final DelayMessageId messageId = DelayMessageId.random(shardId);
                    final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                            1_000, 9_000, lane, OrderingMode.BEST_EFFORT, new byte[]{(byte) index},
                            source.canonicalBytes());
                    final ReadyFixture fixture = readyFixture(laneRecord, messageId, source, message);
                    lanes.add(laneRecord);
                    fixtures.add(fixture);
                    putReady(batch, fixture);
                }
            });
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            lanes.forEach(scheduler::register);
            assertEquals(2, scheduler.rebuildFromAuthoritativeReady(2));
            assertEquals(1, scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000)).size());

            // The cursor wraps to the already-polled first head.  The exact
            // discovered-head identity prevents a second offer while the
            // Claim result is still pending.
            assertEquals(List.of(), scheduler.discoverReady(
                    new SchedulerBudget(1, 1024, 1_000_000_000)));

            final ReadyFixture old = fixtures.get(0);
            final LaneRecord laneRecord = old.lane();
            final SourcePosition nextSource = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                    2, null, 1_002);
            final DelayMessageId nextMessageId = DelayMessageId.random(shardId);
            final MessageRecord nextMessage = new MessageRecord(MessageStatus.SCHEDULED, 2, 1,
                    1_000, 9_000, laneRecord.laneId(), OrderingMode.BEST_EFFORT, new byte[]{9},
                    nextSource.canonicalBytes());
            final ReadyFixture next = readyFixture(laneRecord, nextMessageId, nextSource, nextMessage);
            store.write(batch -> {
                batch.delete(ColumnFamily.TIMELINE, old.readyKey());
                batch.delete(ColumnFamily.TIMELINE, old.timelineKey());
                putReady(batch, next);
            });

            assertEquals(List.of(), scheduler.discoverReady(
                    new SchedulerBudget(1, 1024, 1_000_000_000)));
            final List<ScheduleWorkItem> discovered = scheduler.discoverReady(
                    new SchedulerBudget(1, 1024, 1_000_000_000));
            assertEquals(1, discovered.size());
            assertEquals(nextMessageId, discovered.get(0).messageId());
        }
    }

    @Test
    void readyTransitionWithSameWorkUsesNewReadyKey() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("ready-discovery-lane-version"));
        final DestinationLaneId lane = lane(42);
        final LaneRecord initialLane = new LaneRecord(lane, new byte[16], 1, 1,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 1_000);
        final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                0, null, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                1_000, 9_000, lane, OrderingMode.BEST_EFFORT, new byte[]{1}, source.canonicalBytes());
        final ReadyFixture initial = readyFixture(initialLane, messageId, source, message);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> putReady(batch, initial));
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(initialLane);
            assertEquals(1, scheduler.rebuildFromAuthoritativeReady(1));
            assertEquals(messageId, scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000))
                    .get(0).messageId());
            assertEquals(List.of(), scheduler.discoverReady(
                    new SchedulerBudget(1, 1024, 1_000_000_000)));

            // The Claim/READY transition changes only the Lane version and
            // physical READY key.  Message identity and generation remain
            // unchanged, so work-item equality alone must not suppress it.
            final LaneRecord transitionedLane = new LaneRecord(lane, new byte[16], 1, 2,
                    AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 1_000);
            final ReadyFixture transitioned = readyFixture(transitionedLane, messageId, source, message);
            store.write(batch -> {
                batch.delete(ColumnFamily.TIMELINE, initial.readyKey());
                putReady(batch, transitioned);
            });
            scheduler.register(transitionedLane);

            final List<ScheduleWorkItem> discovered = scheduler.discoverReady(
                    new SchedulerBudget(1, 1024, 1_000_000_000));
            assertEquals(1, discovered.size());
            assertEquals(messageId, discovered.get(0).messageId());
        }
    }

    @Test
    void persistentReadyDiscoveryAndPollFenceFutureDeliverAt() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("ready-discovery-due-boundary"));
        final DestinationLaneId lane = lane(45);
        final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 2_000);
        final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                0, null, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                2_000, 9_000, lane, OrderingMode.BEST_EFFORT, new byte[]{1}, source.canonicalBytes());
        final ReadyFixture fixture = readyFixture(laneRecord, messageId, source, message, 2_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> putReady(batch, fixture));
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(laneRecord);

            assertEquals(List.of(), scheduler.discoverReady(1_999,
                    new SchedulerBudget(1, 1024, 1_000_000_000)));
            assertEquals(List.of(), scheduler.poll(1_999,
                    new SchedulerBudget(1, 1024, 1_000_000_000)));
            final PersistentLaneScheduler recovered = PersistentLaneScheduler.defaults(store);
            recovered.register(laneRecord);
            assertEquals(List.of(messageId), recovered.discoverReady(2_000,
                    new SchedulerBudget(1, 1024, 1_000_000_000)).stream()
                    .map(ScheduleWorkItem::messageId).toList());
            assertEquals(List.of(messageId), recovered.poll(2_000,
                    new SchedulerBudget(1, 1024, 1_000_000_000)).stream()
                    .map(ScheduleWorkItem::messageId).toList());
        }
    }

    @Test
    void persistentRecoveryFirstPassIgnoresFutureLaneForDueFairness() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 34);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("scheduler-recovery-due-fairness"));
        final DestinationLaneId dueLane = lane(46);
        final DestinationLaneId futureLane = lane(47);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(dueLane, 1));
            scheduler.register(record(futureLane, 1));
            scheduler.offer(new ScheduleWorkItem(dueLane, DelayMessageId.random(shardId), 1, 1_000, 1));
            scheduler.offer(new ScheduleWorkItem(futureLane, DelayMessageId.random(shardId), 1, 10_000, 1));

            assertEquals(List.of(dueLane), scheduler.poll(1_000,
                    new SchedulerBudget(1, 1024, 1_000_000_000)).stream()
                    .map(ScheduleWorkItem::laneId).toList());
            scheduler.offer(new ScheduleWorkItem(dueLane, DelayMessageId.random(shardId), 2, 1_001, 1));
            assertEquals(List.of(dueLane), scheduler.poll(1_001,
                    new SchedulerBudget(1, 1024, 1_000_000_000)).stream()
                    .map(ScheduleWorkItem::laneId).toList());
        }
    }

    @Test
    void persistentRecoveryFirstPassDoesNotWaitForAnOversizedHead() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("scheduler-recovery-byte-fairness"));
        final DestinationLaneId oversizedLane = lane(50);
        final DestinationLaneId smallLane = lane(51);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(oversizedLane, 1));
            scheduler.register(record(smallLane, 1));
            scheduler.offer(new ScheduleWorkItem(oversizedLane, DelayMessageId.random(shardId), 1, 1_000, 20));
            scheduler.offer(new ScheduleWorkItem(smallLane, DelayMessageId.random(shardId), 1, 1_000, 1));

            final SchedulerBudget budget = new SchedulerBudget(1, 10, 1_000_000_000);
            assertEquals(List.of(smallLane), scheduler.poll(1_000, budget).stream()
                    .map(ScheduleWorkItem::laneId).toList());

            scheduler.offer(new ScheduleWorkItem(smallLane, DelayMessageId.random(shardId), 2, 1_001, 1));
            assertEquals(List.of(smallLane), scheduler.poll(1_001, budget).stream()
                    .map(ScheduleWorkItem::laneId).toList());
        }
    }

    @Test
    void readyDiscoveryRejectsFirstEntryThatExceedsByteBudget() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 32);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("ready-discovery-byte-budget"));
        final DestinationLaneId lane = lane(43);
        final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 1_000);
        final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                0, null, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                1_000, 9_000, lane, OrderingMode.BEST_EFFORT, new byte[]{1}, source.canonicalBytes());
        final ReadyFixture fixture = readyFixture(laneRecord, messageId, source, message);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> putReady(batch, fixture));
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(laneRecord);

            assertThrows(IllegalStateException.class, () -> scheduler.discoverReady(
                    new SchedulerBudget(1, 1, 1_000_000_000)));
        }
    }

    @Test
    void readyDiscoveryStopsBeforeFirstEntryWhenTimeBudgetIsElapsed() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 33);
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("ready-discovery-time-budget"));
        final DestinationLaneId lane = lane(44);
        final LaneRecord laneRecord = new LaneRecord(lane, new byte[16], 1, 1,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 1_000);
        final SourcePosition source = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                0, null, 1_000);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                1_000, 9_000, lane, OrderingMode.BEST_EFFORT, new byte[]{1}, source.canonicalBytes());
        final ReadyFixture fixture = readyFixture(laneRecord, messageId, source, message);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> putReady(batch, fixture));
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(laneRecord);

            assertEquals(List.of(), scheduler.discoverReady(
                    new SchedulerBudget(1, 1024, 1)));
        }
    }

    private static LaneRecord record(final DestinationLaneId lane, final int weight) {
        return new LaneRecord(lane, new byte[16], 1, 0, AdmissionGate.OPEN, RuntimeReadiness.READY, weight, 0);
    }

    private static LaneRecord recordWithIncarnation(final DestinationLaneId lane, final int marker) {
        final byte[] incarnation = new byte[16];
        incarnation[0] = (byte) marker;
        return new LaneRecord(lane, incarnation, 1, 0, AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
    }

    private static ScheduleWorkItem item(final DestinationLaneId lane, final int generation) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), generation);
        return new ScheduleWorkItem(lane, DelayMessageId.random(shard), generation, generation, 1);
    }

    private static DestinationLaneId lane(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return new DestinationLaneId(bytes);
    }

    private static ReadyFixture readyFixture(final LaneRecord lane, final DelayMessageId messageId,
                                             final SourcePosition source, final MessageRecord message) {
        return readyFixture(lane, messageId, source, message, 1_000);
    }

    private static ReadyFixture readyFixture(final LaneRecord lane, final DelayMessageId messageId,
                                             final SourcePosition source, final MessageRecord message,
                                             final long eligibleAtEpochMs) {
        final byte[] readyKey = KeyCodec.timelineReady(eligibleAtEpochMs, lane.laneId(), lane.laneVersion());
        final byte[] timelineKey = KeyCodec.timelineDue(lane.laneId(), eligibleAtEpochMs,
                source.sourceOrderToken(), messageId,
                message.generation());
        return new ReadyFixture(lane, messageId, message, readyKey, timelineKey,
                new ReadyIndexValue(lane.laneId(), eligibleAtEpochMs, lane.laneVersion(), messageId,
                        message.generation(),
                        Bytes.sha256(timelineKey)));
    }

    private static void putReady(final ShardStore.Batch batch, final ReadyFixture fixture)
            throws org.rocksdb.RocksDBException {
        batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(fixture.lane().laneId()),
                LaneRecordEnvelopeV1.active(fixture.lane().encode()).canonicalBytes());
        batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(fixture.messageId()), fixture.message().encode());
        batch.putValue(ColumnFamily.TIMELINE, 1, fixture.timelineKey(),
                new TimelineEntry(fixture.messageId(), fixture.message().generation()).encode());
        batch.putValue(ColumnFamily.TIMELINE, 3, fixture.readyKey(), fixture.ready().encode());
    }

    private record ReadyFixture(LaneRecord lane, DelayMessageId messageId, MessageRecord message,
                                byte[] readyKey, byte[] timelineKey, ReadyIndexValue ready) {
    }

    private static OwnerIdentityV1 owner(final long epoch) {
        return new OwnerIdentityV1(Bytes.utf8("scheduler-deployment"),
                Bytes.utf8("scheduler-worker-" + epoch), epoch,
                Bytes.sha256(Bytes.utf8("scheduler-owner-" + epoch)));
    }
}

package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DelayShardHeadBudgetTest {
    @TempDir
    Path tempDir;

    @Test
    void deletingHeadCannotPartiallyCommitAtAnyRequiredReadBoundary() {
        try (Fixture fixture = new Fixture(tempDir.resolve("record-boundaries"))) {
            final PreparedCommand cancel =
                    PreparedCommand.cancel(fixture.shardId, fixture.first.delayMessageId(), 0, 30_000);
            final List<String> before = snapshot(fixture.store);
            for (int records : List.of(1, 2, 3, 4)) {
                final DelayShard shard =
                        boundedShard(fixture.store, new HeadReadPolicy(records, 1_000_000, 1_000, () -> 0));
                final long writes = fixture.store.operationStatistics().nativeWriteCalls();
                final long sequence = shard.mutationSequence();
                final HeadReadIncompleteException incomplete =
                        assertThrows(HeadReadIncompleteException.class, () -> shard.apply(cancel, fixture.position(2)));
                assertEquals(BoundedReadBudget.Exhaustion.RECORDS, incomplete.reason());
                assertEquals(records, shard.headPlanReadStatistics().actualRecords());
                assertEquals(before, snapshot(fixture.store));
                assertEquals(writes, fixture.store.operationStatistics().nativeWriteCalls());
                assertEquals(sequence, shard.mutationSequence());
                assertEquals(fixture.position(1), shard.lastAppliedSourcePosition());
                assertNull(shard.getCommandResult(cancel.commandId()));
            }
            final DelayShard retry = boundedShard(fixture.store, new HeadReadPolicy(5, 1_000_000, 1_000, () -> 0));
            assertEquals(
                    StableCode.CANCELED,
                    retry.apply(cancel, fixture.position(2)).stableCode());
            assertEquals(5, retry.headPlanReadStatistics().actualRecords());
            assertEquals(
                    fixture.second.delayMessageId(),
                    retry.discoverReady(10_000, 1).get(0).messageId());
        }
    }

    @Test
    void insertedAndRescheduledCandidatesCannotMaskAnUnreadStoredPrefix() {
        try (Fixture fixture = new Fixture(tempDir.resolve("included-candidate"))) {
            final List<String> before = snapshot(fixture.store);
            for (PreparedCommand command : List.of(
                    fixture.schedule(1_500),
                    PreparedCommand.reschedule(
                            fixture.shardId, fixture.first.delayMessageId(), 0, 1_500, 20_000, 30_000))) {
                final DelayShard shard = boundedShard(fixture.store, new HeadReadPolicy(1, 1_000_000, 1_000, () -> 0));
                assertThrows(HeadReadIncompleteException.class, () -> shard.apply(command, fixture.position(2)));
                assertEquals(before, snapshot(fixture.store));
                assertNull(shard.getCommandResult(command.commandId()));
            }
        }
    }

    @Test
    void candidatePointReadChargesReturnedBytesEvenWhenTheCompleteValueDoesNotFit() {
        try (Fixture fixture = new Fixture(tempDir.resolve("bytes"))) {
            final byte[] laneKey = com.nereusstream.delay.store.KeyCodec.metaLane(fixture.lane);
            final byte[] messageKey = com.nereusstream.delay.store.KeyCodec.idMessage(fixture.second.delayMessageId());
            final List<ShardStore.KeyValue> due = fixture.store.scan(
                    ColumnFamily.TIMELINE,
                    Bytes.concat(new byte[] {1, 1}, fixture.lane.bytes()),
                    Bytes.concat(new byte[] {1, 1}, fixture.lane.bytes(), new byte[] {(byte) 255}),
                    10);
            assertEquals(2, due.size());
            final long laneBytes = laneKey.length + fixture.store.get(ColumnFamily.META, laneKey).length;
            final long messageBytes = messageKey.length + fixture.store.get(ColumnFamily.ID, messageKey).length;
            final long throughMessageBytes = laneBytes
                    + messageBytes
                    + due.stream()
                            .mapToLong(entry -> (long) entry.key().length + entry.value().length)
                            .sum();
            final DelayShard shard =
                    boundedShard(fixture.store, new HeadReadPolicy(100, throughMessageBytes - 1, 1_000, () -> 0));
            final List<String> before = snapshot(fixture.store);
            final PreparedCommand cancel =
                    PreparedCommand.cancel(fixture.shardId, fixture.first.delayMessageId(), 0, 30_000);
            final HeadReadIncompleteException incomplete =
                    assertThrows(HeadReadIncompleteException.class, () -> shard.apply(cancel, fixture.position(2)));
            assertEquals(BoundedReadBudget.Exhaustion.BYTES, incomplete.reason());
            assertEquals(4, shard.headPlanReadStatistics().actualRecords());
            assertEquals(throughMessageBytes, shard.headPlanReadStatistics().actualBytes());
            assertEquals(
                    throughMessageBytes - messageBytes,
                    shard.headPlanReadStatistics().chargedBytes());
            assertEquals(1, shard.headPlanReadStatistics().deniedReads());
            assertEquals(before, snapshot(fixture.store));
            final DelayShard retry = boundedShard(
                    fixture.store, new HeadReadPolicy(100, throughMessageBytes + laneBytes, 1_000, () -> 0));
            assertEquals(
                    StableCode.CANCELED,
                    retry.apply(cancel, fixture.position(2)).stableCode());
            assertEquals(
                    throughMessageBytes + laneBytes,
                    retry.headPlanReadStatistics().actualBytes());
        }
    }

    @Test
    void dueSuccessCannotMaskNativeExhaustionAndRepeatedElapsedYieldsRemainDiagnosable() {
        try (Fixture fixture = new Fixture(tempDir.resolve("native-deadline"))) {
            final AtomicLong clock = new AtomicLong();
            final AtomicBoolean ticking = new AtomicBoolean(true);
            final DelayShard shard = boundedShard(
                    fixture.store,
                    new HeadReadPolicy(100, 1_000_000, 8, () -> ticking.get() ? clock.getAndIncrement() : clock.get()));
            final PreparedCommand cancel =
                    PreparedCommand.cancel(fixture.shardId, fixture.first.delayMessageId(), 0, 30_000);
            final List<String> before = snapshot(fixture.store);
            final long writes = fixture.store.operationStatistics().nativeWriteCalls();
            for (int turn = 1; turn <= 3; turn++) {
                final HeadReadIncompleteException incomplete =
                        assertThrows(HeadReadIncompleteException.class, () -> shard.apply(cancel, fixture.position(2)));
                assertEquals(BoundedReadBudget.Exhaustion.ELAPSED, incomplete.reason());
                assertTrue(Arrays.stream(incomplete.getCause().getStackTrace())
                        .anyMatch(frame -> frame.getMethodName().equals("findLaneNativeCandidate")));
                assertEquals(4, shard.headPlanReadStatistics().actualRecords());
                assertEquals(turn, shard.headPlanReadStatistics().consecutiveElapsedYields());
                assertEquals(before, snapshot(fixture.store));
                assertEquals(writes, fixture.store.operationStatistics().nativeWriteCalls());
            }
            ticking.set(false);
            assertEquals(
                    StableCode.CANCELED,
                    shard.apply(cancel, fixture.position(2)).stableCode());
            assertEquals(0, shard.headPlanReadStatistics().consecutiveElapsedYields());
        }
    }

    @Test
    void retryClassificationCannotHideAnEarlierWriteInAnEnclosingMutation() {
        try (Fixture fixture = new Fixture(tempDir.resolve("outer-write"))) {
            final DelayShard shard = boundedShard(fixture.store, new HeadReadPolicy(1, 1_000_000, 1_000, () -> 0));
            final PreparedCommand cancel =
                    PreparedCommand.cancel(fixture.shardId, fixture.first.delayMessageId(), 0, 30_000);
            final IllegalStateException failure = assertThrows(
                    IllegalStateException.class,
                    () -> shard.runReadRetryableMutation(() -> {
                        // An enclosing action that already wrote cannot reuse the
                        // nested head-plan proof as whole-action retry authority.
                        fixture.store.write(batch -> {});
                        return shard.apply(cancel, fixture.position(2));
                    }));
            assertTrue(failure.getMessage().contains("view changed"));
            assertNull(shard.getCommandResult(cancel.commandId()));
        }
    }

    private static DelayShard boundedShard(final ShardStore store, final HeadReadPolicy policy) {
        return new DelayShard(
                store,
                DelayShardConfig.defaults(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                policy);
    }

    private static List<String> snapshot(final ShardStore store) {
        final List<String> result = new ArrayList<>();
        for (ColumnFamily family : ColumnFamily.values()) {
            for (ShardStore.KeyValue entry : store.scan(family, null, null, 1_000)) {
                result.add(family.name() + ":" + Bytes.hex(entry.key()) + ":" + Bytes.hex(entry.value()));
            }
        }
        return List.copyOf(result);
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        private final UUID topic = UUID.randomUUID();
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("head-budget"));
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final PreparedCommand first;
        private final PreparedCommand second;

        private Fixture(final Path path) {
            final ShardStoreConfig config = ShardStoreConfig.defaults(path);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shardId, resources);
            final DelayShard seed = new DelayShard(store, DelayShardConfig.defaults());
            first = schedule(2_000);
            second = schedule(3_000);
            assertEquals(StableCode.SCHEDULED, seed.apply(first, position(0)).stableCode());
            seed.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(StableCode.SCHEDULED, seed.apply(second, position(1)).stableCode());
        }

        private PreparedCommand schedule(final long deliverAt) {
            return PreparedCommand.schedule(
                    shardId,
                    new ScheduleIntent(lane, deliverAt, 20_000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")),
                    30_000);
        }

        private KafkaSourcePosition position(final long offset) {
            return new KafkaSourcePosition(shardId, "head-budget-cluster", topic, offset, null, 1_000);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}

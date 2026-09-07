package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The deliberately exhaustive oracle stays in tests when the production head algorithm changes. */
class DelayShardHeadSelectionTest {
    @TempDir
    Path tempDir;

    @Test
    void committedHeadsMatchFullScanReferenceAcrossMutationsAndClaimRollback() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reference"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("head-reference"));
        final Random random = new Random(20260907);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final List<PreparedCommand> messages = new ArrayList<>();
            long offset = 0;
            for (int index = 0; index < 48; index++) {
                final PreparedCommand command = PreparedCommand.schedule(
                        shardId,
                        new ScheduleIntent(
                                lane,
                                2_000 + 100L * random.nextInt(8),
                                20_000,
                                index % 2 == 0 ? OrderingMode.BEST_EFFORT : OrderingMode.DELIVERY_TIME_FIFO,
                                Bytes.utf8("payload")),
                        30_000);
                messages.add(command);
                assertEquals(
                        StableCode.SCHEDULED,
                        shard.apply(command, position(shardId, offset++)).stableCode());
                if (index == 0) {
                    shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
                }
                assertReference(shard, store, lane);
            }
            for (int index = 0; index < 20; index++) {
                final PreparedCommand command = messages.get(index);
                if (index % 3 == 0) {
                    assertEquals(
                            StableCode.CANCELED,
                            shard.apply(
                                            PreparedCommand.cancel(shardId, command.delayMessageId(), 0, 30_000),
                                            position(shardId, offset++))
                                    .stableCode());
                } else {
                    assertEquals(
                            StableCode.SUPERSEDED,
                            shard.apply(
                                            PreparedCommand.reschedule(
                                                    shardId,
                                                    command.delayMessageId(),
                                                    0,
                                                    index % 2 == 0 ? 1_500 : 4_000,
                                                    20_000,
                                                    30_000),
                                            position(shardId, offset++))
                                    .stableCode());
                }
                assertReference(shard, store, lane);
            }
            final DelayMessageId selected =
                    shard.discoverReady(10_000, 1).get(0).messageId();
            final AuthorIdentity owner = AuthorIdentity.owner(
                    Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, Bytes.sha256(Bytes.utf8("owner")));
            final byte[] charge = new PublishAdmissionBody.ChargeVector(
                            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                    .canonicalBytes();
            final ClaimRecord claim = shard.claimForPublish(selected, owner, 10_000, new byte[0], charge);
            assertEquals(MessageStatus.CLAIMED, shard.getMessage(selected).status());
            assertReference(shard, store, lane);
            shard.revokeClaim(claim.claimId(), owner.generation());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(selected).status());
            assertReference(shard, store, lane);
        }
    }

    @Test
    void smallBacklogMeasurementSeparatesCandidateReadsFromMessageGetsAndQuotaBytes() {
        for (int count : new int[] {16, 64}) {
            final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("measurement-" + count));
            final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
            final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("head-measurement"));
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                    ShardStore store = ShardStore.open(config, shardId, resources)) {
                final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
                final long quotaBefore = store.operationStatistics().quotaPreparedPutBytes();
                for (int index = 0; index < count; index++) {
                    final PreparedCommand command = PreparedCommand.schedule(
                            shardId,
                            new ScheduleIntent(
                                    lane, 2_000 + index, 20_000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")),
                            30_000);
                    assertEquals(
                            StableCode.SCHEDULED,
                            shard.apply(command, position(shardId, index)).stableCode());
                }
                final DelayShard.HeadReadStatistics reads = shard.headReadStatistics();
                assertEquals(reads.candidateKeysRead(), reads.messageGets());
                assertTrue(reads.candidateKeysRead() > 0);
                final long quotaBytes = store.operationStatistics().quotaPreparedPutBytes() - quotaBefore;
                assertTrue(quotaBytes > 0);
                assertEquals(0, store.operationStatistics().schedulerPreparedPutBytes());
                System.out.printf(
                        "HEAD_MEASUREMENT N=%d L=1 candidateKeys=%d messageGets=%d quotaPreparedBytes=%d%n",
                        count, reads.candidateKeysRead(), reads.messageGets(), quotaBytes);
            }
        }
    }

    private static void assertReference(final DelayShard shard, final ShardStore store, final DestinationLaneId lane) {
        final List<ShardStore.KeyValue> all = new ArrayList<>();
        for (byte tag : new byte[] {1, 2}) {
            final byte[] prefix = Bytes.concat(new byte[] {tag, 1}, lane.bytes());
            final byte[] upper = Bytes.concat(new byte[] {tag, 1}, lane.bytes(), new byte[] {(byte) 255});
            all.addAll(store.scan(ColumnFamily.TIMELINE, prefix, upper, 1_000));
        }
        // Exactly the original comparator: namespace-specific stored time, then full unsigned key.
        all.sort(Comparator.comparingLong((ShardStore.KeyValue entry) ->
                        ByteBuffer.wrap(entry.key()).getLong(34))
                .thenComparing(ShardStore.KeyValue::key, Arrays::compareUnsigned));
        final var ready = shard.discoverReady(100_000, 10);
        if (all.isEmpty()) {
            assertTrue(ready.isEmpty());
            return;
        }
        assertEquals(1, ready.size());
        final byte[] key = all.get(0).key();
        final DelayMessageId expected = new DelayMessageId(Arrays.copyOfRange(
                key, key.length - Integer.BYTES - DelayMessageId.LENGTH, key.length - Integer.BYTES));
        assertEquals(expected, ready.get(0).messageId());
        assertEquals(
                ByteBuffer.wrap(key).getInt(key.length - Integer.BYTES),
                ready.get(0).generation());
        final ReadyIndexValue projection = ReadyIndexValue.decode(store.getValue(
                        ColumnFamily.TIMELINE,
                        KeyCodec.timelineReady(
                                ready.get(0).nextEligibleAtEpochMs(),
                                lane,
                                ready.get(0).laneVersion()),
                        3)
                .payload());
        assertArrayEquals(Bytes.sha256(key), projection.timelineKeySha256());
        assertEquals(MessageStatus.SCHEDULED, shard.getMessage(expected).status());
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset) {
        return new KafkaSourcePosition(
                shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("head-source")), offset, 1, 1_000);
    }
}

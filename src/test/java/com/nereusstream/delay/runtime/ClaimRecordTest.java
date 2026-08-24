package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClaimRecordTest {
    @TempDir
    Path tempDir;

    @Test
    void decodeRejectsTruncatedLengthPrefixesAsValidationErrors() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-record-truncation"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-record-truncation-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(
                shardId,
                new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-record-truncation")),
                9_000);
        final KafkaSourcePosition position =
                new KafkaSourcePosition(shardId, "cluster", java.util.UUID.randomUUID(), 0, 0, 1_000);
        final AuthorIdentity owner = AuthorIdentity.owner(
                Bytes.utf8("claim-record-deployment"),
                Bytes.utf8("claim-record-worker"),
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("claim-record-fence")));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            shard.apply(schedule, position);
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim =
                    shard.claimForPublish(schedule.delayMessageId(), owner, 2_500, new byte[0], chargeVector());
            final byte[] encoded = claim.encode();
            for (int length = 0; length < encoded.length; length++) {
                final byte[] truncated = Arrays.copyOf(encoded, length);
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ClaimRecord.decode(truncated),
                        "truncated Claim record length=" + length);
            }
            assertEquals(claim, ClaimRecord.decode(encoded));
            assertEquals(
                    claim,
                    ClaimRecord.decode(store.getValue(ColumnFamily.INFLIGHT, claim.encodedKey(), ClaimRecord.VALUE_TYPE)
                            .payload()));
        }
    }

    @Test
    void claimRejectsTimelineKeyForAnotherMessageAfterPreconditionHashIsRebound() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-record-key-identity"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-record-key-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(
                shardId,
                new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-record-key")),
                9_000);
        final KafkaSourcePosition position =
                new KafkaSourcePosition(shardId, "cluster", java.util.UUID.randomUUID(), 0, 0, 1_000);
        final AuthorIdentity owner = AuthorIdentity.owner(
                Bytes.utf8("claim-key-deployment"),
                Bytes.utf8("claim-key-worker"),
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("claim-key-fence")));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            shard.apply(schedule, position);
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim =
                    shard.claimForPublish(schedule.delayMessageId(), owner, 2_500, new byte[0], chargeVector());
            final DelayMessageId otherMessage = DelayMessageId.random(shardId);
            final byte[] reboundTimelineKey =
                    KeyCodec.timelineDue(lane, 2_000, position.sourceOrderToken(), otherMessage, claim.generation());
            final byte[] reboundPrecondition = replaceTimelineHash(claim.preconditionBytes(), reboundTimelineKey);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new ClaimRecord(
                            claim.delayMessageId(),
                            claim.generation(),
                            claim.claimId(),
                            claim.ownerEpoch(),
                            claim.claimSequence(),
                            claim.laneId(),
                            claim.laneIncarnation(),
                            claim.laneControlVersion(),
                            claim.runtimeLaneVersion(),
                            claim.ownerIdentity(),
                            claim.storeIncarnation(),
                            reboundPrecondition,
                            reboundTimelineKey,
                            claim.runtimeRevision(),
                            ClaimRecord.computeInstanceDigest(
                                    reboundPrecondition, reboundTimelineKey, claim.runtimeRevision())));
        }
    }

    private static byte[] replaceTimelineHash(final byte[] precondition, final byte[] timelineKey) {
        final List<CanonicalProtobuf.Reader.Field> fields = new java.util.ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(precondition);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : fields) {
                if (field.number() == 9) {
                    CanonicalProtobuf.bytes(output, 9, Bytes.sha256(timelineKey));
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }
}

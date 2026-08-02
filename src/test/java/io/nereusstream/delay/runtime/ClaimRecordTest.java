package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClaimRecordTest {
    @TempDir
    Path tempDir;

    @Test
    void decodeRejectsTruncatedLengthPrefixesAsValidationErrors() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-record-truncation"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-record-truncation-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-record-truncation")), 9_000);
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", java.util.UUID.randomUUID(),
                0, 0, 1_000);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("claim-record-deployment"),
                Bytes.utf8("claim-record-worker"), 42, Bytes.sha256(Bytes.utf8("claim-record-fence")));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            shard.apply(schedule, position);
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 2_500,
                    new byte[0], chargeVector());
            final byte[] encoded = claim.encode();
            for (int length = 0; length < encoded.length; length++) {
                final byte[] truncated = Arrays.copyOf(encoded, length);
                assertThrows(IllegalArgumentException.class, () -> ClaimRecord.decode(truncated),
                        "truncated Claim record length=" + length);
            }
            assertEquals(claim, ClaimRecord.decode(encoded));
            assertEquals(claim, ClaimRecord.decode(store.getValue(ColumnFamily.INFLIGHT,
                    claim.encodedKey(), ClaimRecord.VALUE_TYPE).payload()));
        }
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }
}

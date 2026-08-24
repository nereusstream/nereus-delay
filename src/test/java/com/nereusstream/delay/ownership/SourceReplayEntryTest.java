package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SourceReplayEntryTest {
    @Test
    void commandReplayRecordRequiresThePositionShard() {
        final ShardId commandShard = new ShardId(RouteIncarnation.random(), 1);
        final ShardId positionShard = new ShardId(RouteIncarnation.random(), 1);
        final PreparedCommand command = PreparedCommand.schedule(
                commandShard,
                new ScheduleIntent(
                        com.nereusstream.delay.protocol.DestinationLaneId.derive(
                                Bytes.utf8("replay-entry-command-lane")),
                        2_000,
                        5_000,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload")),
                9_000);
        assertThrows(
                IllegalArgumentException.class,
                () -> new SourceReplayRecord(command, kafkaPosition(positionShard), null, null));
    }

    @Test
    void mutationReplayRecordRequiresThePositionShard() throws Exception {
        final ShardId mutationShard = new ShardId(RouteIncarnation.random(), 2);
        final ShardId positionShard = new ShardId(RouteIncarnation.random(), 2);
        final SystemMutation mutation = timeFence(mutationShard);
        assertThrows(
                IllegalArgumentException.class,
                () -> new SourceReplayMutation(mutation, kafkaPosition(positionShard), null, null));
    }

    private static KafkaSourcePosition kafkaPosition(final ShardId shard) {
        return new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 0, null, 1_000);
    }

    private static SystemMutation timeFence(final ShardId shard) throws Exception {
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(
                2_000,
                2_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("replay-entry-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("replay-entry-proof")),
                0,
                null);
        final byte[] proofId = Bytes.sha256(Bytes.utf8("replay-entry-proof-id"));
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shard).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, 2_000);
            CanonicalProtobuf.uint32(output, 11, 1);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof.canonicalBytes());
        });
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        return SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                9_000,
                proofId,
                body,
                AuthorIdentity.fence(Bytes.utf8("replay-entry-fence"), 1).canonicalBytes(),
                1,
                keyPair.getPrivate());
    }
}

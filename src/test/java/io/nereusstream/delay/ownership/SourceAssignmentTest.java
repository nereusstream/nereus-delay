package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SourceAssignmentTest {
    @Test
    void assignmentBindsShardAndBarrierAndCopiesIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0);
        final byte[] identity = Bytes.sha256(Bytes.utf8("assignment"));
        final SourceAssignment assignment = new SourceAssignment(shard, identity, 4, barrier);
        identity[0] ^= 1;
        assertArrayEquals(Bytes.sha256(Bytes.utf8("assignment")), assignment.assignmentId());
    }

    @Test
    void assignmentRejectsWrongShardAndZeroIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0);
        assertThrows(IllegalArgumentException.class,
                () -> new SourceAssignment(shard, new byte[32], 1, barrier));
        assertThrows(IllegalArgumentException.class,
                () -> new SourceAssignment(new ShardId(RouteIncarnation.random(), 2),
                        Bytes.sha256(Bytes.utf8("assignment")), 1, barrier));
    }
}

package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShardSubjectV1Test {
    @Test
    void canonicalSubjectRoundTripsAndProjectsToShardId() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final ShardSubjectV1 subject = new ShardSubjectV1(shard);
        assertEquals(subject, ShardSubjectV1.decode(subject.canonicalBytes()));
        assertEquals(shard, subject.shardId());
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32be(shard.partition())),
                subject.canonicalHashBytes());
    }

    @Test
    void rejectsPartitionOverflowAndUnknownFields() {
        final ShardSubjectV1 subject = new ShardSubjectV1(RouteIncarnation.random(), 1);
        final byte[] overflow = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject.routeIncarnation().bytes());
            CanonicalProtobuf.uint64(output, 2, (long) Integer.MAX_VALUE + 1);
        });
        assertThrows(IllegalArgumentException.class, () -> ShardSubjectV1.decode(overflow));

        final byte[] unknown = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, subject.partition());
            CanonicalProtobuf.uint32(output, 3, 1);
        });
        assertThrows(IllegalArgumentException.class, () -> ShardSubjectV1.decode(unknown));
    }
}

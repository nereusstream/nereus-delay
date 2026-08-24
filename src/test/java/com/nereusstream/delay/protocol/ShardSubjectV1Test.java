package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ShardSubjectV1Test {
    @Test
    void canonicalSubjectRoundTripsAndProjectsToShardId() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final ShardSubjectV1 subject = new ShardSubjectV1(shard);
        assertEquals(subject, ShardSubjectV1.decode(subject.canonicalBytes()));
        assertEquals(shard, subject.shardId());
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition())),
                subject.canonicalHashBytes());
    }

    @Test
    void preservesUnsignedHighBitPartition() {
        final ShardSubjectV1 subject = new ShardSubjectV1(RouteIncarnation.random(), -1);
        assertEquals(subject, ShardSubjectV1.decode(subject.canonicalBytes()));
        assertEquals(4_294_967_295L, subject.shardId().unsignedPartition());
    }

    @Test
    void rejectsPartitionOverflowAndUnknownFields() {
        final ShardSubjectV1 subject = new ShardSubjectV1(RouteIncarnation.random(), 1);
        final byte[] overflow = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject.routeIncarnation().bytes());
            CanonicalProtobuf.uint64(output, 2, 0x1_0000_0000L);
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

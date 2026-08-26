package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ShardSubjectTest {
    @Test
    void canonicalSubjectRoundTripsAndProjectsToShardId() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final ShardSubject subject = new ShardSubject(shard);
        assertEquals(subject, ShardSubject.decode(subject.canonicalBytes()));
        assertEquals(shard, subject.shardId());
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition())),
                subject.canonicalHashBytes());
    }

    @Test
    void preservesUnsignedHighBitPartition() {
        final ShardSubject subject = new ShardSubject(RouteIncarnation.random(), -1);
        assertEquals(subject, ShardSubject.decode(subject.canonicalBytes()));
        assertEquals(4_294_967_295L, subject.shardId().unsignedPartition());
    }

    @Test
    void rejectsPartitionOverflowAndUnknownFields() {
        final ShardSubject subject = new ShardSubject(RouteIncarnation.random(), 1);
        final byte[] overflow = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject.routeIncarnation().bytes());
            CanonicalProtobuf.uint64(output, 2, 0x1_0000_0000L);
        });
        assertThrows(IllegalArgumentException.class, () -> ShardSubject.decode(overflow));

        final byte[] unknown = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, subject.partition());
            CanonicalProtobuf.uint32(output, 3, 1);
        });
        assertThrows(IllegalArgumentException.class, () -> ShardSubject.decode(unknown));
    }
}

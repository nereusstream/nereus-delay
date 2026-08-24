package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SourceAssignmentCodecTest {
    @Test
    void kafkaAssignmentRoundTripsWithCanonicalBytes() {
        final ShardId shard = shard(3);
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("kafka-assignment")),
                7,
                new KafkaActivationBarrier(
                        shard, "cluster-a", UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), 19));

        final byte[] encoded = assignment.canonicalBytes();
        final SourceAssignment decoded = SourceAssignment.decode(encoded);

        assertEquals(assignment, decoded);
        assertArrayEquals(encoded, decoded.canonicalBytes());
    }

    @Test
    void pulsarAssignmentRoundTripsBothEmptyAndBatchBarriers() {
        final ShardId shard = shard(4);
        final byte[] resource = Bytes.sha256(Bytes.utf8("pulsar-resource"));
        final byte[] attestation = Bytes.sha256(Bytes.utf8("guard-attestation"));
        final SourceAssignment batch = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("pulsar-batch-assignment")),
                8,
                new PulsarActivationBarrier(
                        shard, resource, "persistent://public/default/topic", 12, 34, 2, 5, 9, attestation, false));
        final SourceAssignment empty = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("pulsar-empty-assignment")),
                9,
                PulsarActivationBarrier.empty(shard, resource, "persistent://public/default/topic", 10, attestation));

        assertEquals(batch, SourceAssignment.decode(batch.canonicalBytes()));
        assertEquals(empty, SourceAssignment.decode(empty.canonicalBytes()));
    }

    @Test
    void decoderRejectsReorderedFieldsAndABarrierForAnotherShard() {
        final ShardId shard = shard(5);
        final byte[] assignmentId = Bytes.sha256(Bytes.utf8("assignment"));
        final byte[] barrier = kafkaBarrierBytes(new ShardId(shard.routeIncarnation(), 6));
        final byte[] foreignBarrier = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, 6);
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("cluster-a"));
            CanonicalProtobuf.bytes(output, 4, uuidBytes(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")));
            CanonicalProtobuf.uint64Bits(output, 5, 1);
        });
        final byte[] foreignOuterBarrier =
                CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, foreignBarrier));
        final byte[] validShape = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, shard.partition());
            CanonicalProtobuf.bytes(output, 3, assignmentId);
            CanonicalProtobuf.uint64Bits(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, foreignOuterBarrier);
        });
        final byte[] reordered = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32Bits(output, 2, shard.partition());
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.bytes(output, 3, assignmentId);
            CanonicalProtobuf.uint64Bits(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, barrier);
        });

        assertThrows(IllegalArgumentException.class, () -> SourceAssignment.decode(reordered));
        assertThrows(IllegalArgumentException.class, () -> SourceAssignment.decode(validShape));
    }

    private static byte[] kafkaBarrierBytes(final ShardId shard) {
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, shard.partition());
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("cluster-a"));
            CanonicalProtobuf.bytes(output, 4, uuidBytes(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")));
            CanonicalProtobuf.uint64Bits(output, 5, 1);
        });
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, body));
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static ShardId shard(final int partition) {
        final byte[] route = new byte[RouteIncarnation.LENGTH];
        for (int index = 0; index < route.length; index++) {
            route[index] = (byte) (index + 1);
        }
        return new ShardId(new RouteIncarnation(route), partition);
    }
}

package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RecoveryPinTest {
    @Test
    void roundTripsSessionBoundPin() {
        final byte[] lineage = bytes(16, 1);
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 6)), 17);
        final RecoveryFloorRef floor = floor(lineage, 7, shard);
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT, lineage, bytes(16, 3), bytes(32, 4), null);
        final RecoveryPin pin = new RecoveryPin(
                bytes(16, 5),
                new ShardSubject(shard),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker-run"), 42, bytes(32, 7)),
                candidate,
                floor,
                7,
                bytes(32, 8));

        assertEquals(pin, RecoveryPin.decode(pin.canonicalBytes()));
        assertEquals(7, pin.observedCatalogGeneration());
    }

    @Test
    void observedCatalogGenerationPreservesCompleteUnsigned64BitPattern() {
        final byte[] lineage = bytes(16, 21);
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 26)), 17);
        final RecoveryFloorRef floor = floor(lineage, Long.MIN_VALUE, shard);
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT, lineage, bytes(16, 23), bytes(32, 24), null);
        final RecoveryPin pin = new RecoveryPin(
                bytes(16, 25), new ShardSubject(shard), owner(), candidate, floor, Long.MIN_VALUE, bytes(32, 27));

        final RecoveryPin decoded = RecoveryPin.decode(pin.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.observedCatalogGeneration());
        assertEquals(Long.MIN_VALUE, decoded.observedFloor().catalogGeneration());
        assertEquals(pin, decoded);
    }

    @Test
    void rejectsLineageGenerationAndDigestDrift() {
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 6)), 17);
        final RecoveryFloorRef floor = floor(bytes(16, 1), 7, shard);
        final RecoveryCandidateRef otherLineage = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT, bytes(16, 2), bytes(16, 3), bytes(32, 4), null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryPin(
                        bytes(16, 5), new ShardSubject(shard), owner(), otherLineage, floor, 7, bytes(32, 8)));

        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT, bytes(16, 1), bytes(16, 3), bytes(32, 4), null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryPin(
                        bytes(16, 5), new ShardSubject(shard), owner(), candidate, floor, 8, bytes(32, 8)));

        final RecoveryPin pin =
                new RecoveryPin(bytes(16, 5), new ShardSubject(shard), owner(), candidate, floor, 7, bytes(32, 8));
        final byte[] tampered = pin.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryPin.decode(tampered));

        final ShardId foreignShard = new ShardId(new RouteIncarnation(bytes(16, 7)), 17);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryPin(
                        bytes(16, 6), new ShardSubject(foreignShard), owner(), candidate, floor, 7, bytes(32, 9)));
    }

    private static RecoveryFloorRef floor(final byte[] lineage, final long generation, final ShardId shard) {
        final UUID topic = UUID.randomUUID();
        return new RecoveryFloorRef(
                lineage,
                bytes(16, 9),
                bytes(32, 10),
                generation,
                new KafkaSourcePosition(shard, "cluster-a", topic, 100, 3, 1_000),
                12,
                List.of());
    }

    private static OwnerIdentity owner() {
        return new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker-run"), 42, bytes(32, 7));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}

package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoveryPinV1Test {
    @Test
    void roundTripsSessionBoundPin() {
        final byte[] lineage = bytes(16, 1);
        final RecoveryFloorRefV1 floor = floor(lineage, 7);
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, lineage, bytes(16, 3), bytes(32, 4), null);
        final RecoveryPinV1 pin = new RecoveryPinV1(bytes(16, 5), new ShardSubjectV1(
                new RouteIncarnation(bytes(16, 6)), 17), new OwnerIdentityV1(
                Bytes.utf8("deployment"), Bytes.utf8("worker-run"), 42, bytes(32, 7)), candidate, floor, 7,
                bytes(32, 8));

        assertEquals(pin, RecoveryPinV1.decode(pin.canonicalBytes()));
        assertEquals(7, pin.observedCatalogGeneration());
    }

    @Test
    void observedCatalogGenerationPreservesCompleteUnsigned64BitPattern() {
        final byte[] lineage = bytes(16, 21);
        final RecoveryFloorRefV1 floor = floor(lineage, Long.MIN_VALUE);
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, lineage, bytes(16, 23), bytes(32, 24), null);
        final RecoveryPinV1 pin = new RecoveryPinV1(bytes(16, 25), new ShardSubjectV1(
                new RouteIncarnation(bytes(16, 26)), 17), owner(), candidate, floor, Long.MIN_VALUE,
                bytes(32, 27));

        final RecoveryPinV1 decoded = RecoveryPinV1.decode(pin.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.observedCatalogGeneration());
        assertEquals(Long.MIN_VALUE, decoded.observedFloor().catalogGeneration());
        assertEquals(pin, decoded);
    }

    @Test
    void rejectsLineageGenerationAndDigestDrift() {
        final RecoveryFloorRefV1 floor = floor(bytes(16, 1), 7);
        final RecoveryCandidateRefV1 otherLineage = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, bytes(16, 2), bytes(16, 3), bytes(32, 4), null);
        assertThrows(IllegalArgumentException.class, () -> new RecoveryPinV1(bytes(16, 5),
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 6)), 17), owner(), otherLineage, floor, 7,
                bytes(32, 8)));

        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, bytes(16, 1), bytes(16, 3), bytes(32, 4), null);
        assertThrows(IllegalArgumentException.class, () -> new RecoveryPinV1(bytes(16, 5),
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 6)), 17), owner(), candidate, floor, 8,
                bytes(32, 8)));

        final RecoveryPinV1 pin = new RecoveryPinV1(bytes(16, 5),
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 6)), 17), owner(), candidate, floor, 7,
                bytes(32, 8));
        final byte[] tampered = pin.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryPinV1.decode(tampered));
    }

    private static RecoveryFloorRefV1 floor(final byte[] lineage, final long generation) {
        final UUID topic = UUID.randomUUID();
        return new RecoveryFloorRefV1(lineage, bytes(16, 9), bytes(32, 10), generation,
                new KafkaSourcePosition(new ShardId(RouteIncarnation.random(), 17), "cluster-a", topic,
                        100, 3, 1_000), 12, List.of());
    }

    private static OwnerIdentityV1 owner() {
        return new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker-run"), 42, bytes(32, 7));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}

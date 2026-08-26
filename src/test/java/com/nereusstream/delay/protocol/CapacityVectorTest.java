package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class CapacityVectorTest {
    @Test
    void encodesAllDimensionsExplicitlyAndRoundTrips() {
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[CapacityDimension.ACTIVE_MESSAGES.wireValue() - 1] = 7;
        amounts[CapacityDimension.CONTROL_RESERVE_BYTES.wireValue() - 1] = 4096;
        amounts[CapacityDimension.OBJECT_BYTES.wireValue() - 1] = 99;
        final CapacityVector vector = new CapacityVector(amounts);

        assertEquals(CapacityDimension.COUNT, CapacityDimension.values().length);
        assertEquals(7, vector.amount(CapacityDimension.ACTIVE_MESSAGES));
        assertEquals(4096, vector.amount(CapacityDimension.CONTROL_RESERVE_BYTES));
        assertFalse(vector.isZero());
        assertEquals(vector, CapacityVector.decode(vector.canonicalBytes()));
        assertArrayEquals(
                vector.digest(), CapacityVector.decode(vector.canonicalBytes()).digest());
    }

    @Test
    void vectorArithmeticIsCheckedDimensionByDimension() {
        final long[] leftAmounts = new long[CapacityDimension.COUNT];
        leftAmounts[0] = 3;
        final long[] rightAmounts = new long[CapacityDimension.COUNT];
        rightAmounts[0] = 2;
        final CapacityVector left = new CapacityVector(leftAmounts);
        final CapacityVector right = new CapacityVector(rightAmounts);

        assertEquals(5, left.add(right).amount(CapacityDimension.ACTIVE_MESSAGES));
        assertEquals(1, left.subtract(right).amount(CapacityDimension.ACTIVE_MESSAGES));
        assertTrue(left.covers(right));
        assertFalse(right.covers(left));
        assertThrows(IllegalStateException.class, () -> right.subtract(left));

        leftAmounts[0] = Long.MAX_VALUE;
        assertThrows(ArithmeticException.class, () -> new CapacityVector(leftAmounts).add(right));
    }

    @Test
    void rejectsDigestDriftAndIncompleteOrUnorderedDimensions() {
        final CapacityVector vector = CapacityVector.empty();
        final byte[] tampered = vector.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CapacityVector.decode(tampered));

        final byte[] incomplete = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, amount(1, 0));
            CanonicalProtobuf.bytes(output, 3, new byte[32]);
        });
        assertThrows(IllegalArgumentException.class, () -> CapacityVector.decode(incomplete));

        final byte[] unordered = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            for (int dimension : new int[] {1, 3, 2}) {
                CanonicalProtobuf.bytes(output, 2, amount(dimension, 0));
            }
            CanonicalProtobuf.bytes(output, 3, new byte[32]);
        });
        assertThrows(IllegalArgumentException.class, () -> CapacityVector.decode(unordered));
    }

    @Test
    void grantBindsKindIdentitySourceVersionVectorAndDigest() {
        final CapacityVector vector = CapacityVector.empty();
        final byte[] grantId = Bytes.sha256(Bytes.utf8("grant"));
        final CapacityGrant grant = new CapacityGrant(CapacityGrantKind.NON_OUTCOME_CONTROL, grantId, 42, vector);

        assertEquals(grant, CapacityGrant.decode(grant.canonicalBytes()));
        final byte[] tampered = grant.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CapacityGrant.decode(tampered));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CapacityGrant(CapacityGrantKind.OUTCOME_RESERVE, new byte[32], 1, vector));
        assertThrows(
                IllegalArgumentException.class,
                () -> CapacityGrant.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint32(output, 1, 1);
                    CanonicalProtobuf.bytes(output, 2, new byte[32]);
                    CanonicalProtobuf.uint64(output, 3, 1);
                    CanonicalProtobuf.bytes(output, 4, vector.canonicalBytes());
                    CanonicalProtobuf.bytes(output, 5, new byte[32]);
                })));
    }

    @Test
    void capacityGrantPreservesCompleteUnsigned64BitSourceVersion() {
        final CapacityGrant grant = new CapacityGrant(
                CapacityGrantKind.NON_OUTCOME_CONTROL,
                Bytes.sha256(Bytes.utf8("high-bit-grant")),
                Long.MIN_VALUE,
                CapacityVector.empty());

        final CapacityGrant decoded = CapacityGrant.decode(grant.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.reserveSourceVersion());
        assertEquals(grant, decoded);
    }

    private static byte[] amount(final int dimension, final long value) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, dimension);
            CanonicalProtobuf.uint64(output, 2, value);
        });
    }
}

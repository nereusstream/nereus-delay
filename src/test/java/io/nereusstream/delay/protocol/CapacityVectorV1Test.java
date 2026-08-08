package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CapacityVectorV1Test {
    @Test
    void encodesAllDimensionsExplicitlyAndRoundTrips() {
        final long[] amounts = new long[CapacityDimensionV1.COUNT];
        amounts[CapacityDimensionV1.ACTIVE_MESSAGES.wireValue() - 1] = 7;
        amounts[CapacityDimensionV1.CONTROL_RESERVE_BYTES.wireValue() - 1] = 4096;
        amounts[CapacityDimensionV1.OBJECT_BYTES.wireValue() - 1] = 99;
        final CapacityVectorV1 vector = new CapacityVectorV1(amounts);

        assertEquals(CapacityDimensionV1.COUNT, CapacityDimensionV1.values().length);
        assertEquals(7, vector.amount(CapacityDimensionV1.ACTIVE_MESSAGES));
        assertEquals(4096, vector.amount(CapacityDimensionV1.CONTROL_RESERVE_BYTES));
        assertFalse(vector.isZero());
        assertEquals(vector, CapacityVectorV1.decode(vector.canonicalBytes()));
        assertArrayEquals(vector.digest(), CapacityVectorV1.decode(vector.canonicalBytes()).digest());
    }

    @Test
    void vectorArithmeticIsCheckedDimensionByDimension() {
        final long[] leftAmounts = new long[CapacityDimensionV1.COUNT];
        leftAmounts[0] = 3;
        final long[] rightAmounts = new long[CapacityDimensionV1.COUNT];
        rightAmounts[0] = 2;
        final CapacityVectorV1 left = new CapacityVectorV1(leftAmounts);
        final CapacityVectorV1 right = new CapacityVectorV1(rightAmounts);

        assertEquals(5, left.add(right).amount(CapacityDimensionV1.ACTIVE_MESSAGES));
        assertEquals(1, left.subtract(right).amount(CapacityDimensionV1.ACTIVE_MESSAGES));
        assertTrue(left.covers(right));
        assertFalse(right.covers(left));
        assertThrows(IllegalStateException.class, () -> right.subtract(left));

        leftAmounts[0] = Long.MAX_VALUE;
        assertThrows(ArithmeticException.class, () -> new CapacityVectorV1(leftAmounts).add(right));
    }

    @Test
    void rejectsDigestDriftAndIncompleteOrUnorderedDimensions() {
        final CapacityVectorV1 vector = CapacityVectorV1.empty();
        final byte[] tampered = vector.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CapacityVectorV1.decode(tampered));

        final byte[] incomplete = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, amount(1, 0));
            CanonicalProtobuf.bytes(output, 3, new byte[32]);
        });
        assertThrows(IllegalArgumentException.class, () -> CapacityVectorV1.decode(incomplete));

        final byte[] unordered = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            for (int dimension : new int[]{1, 3, 2}) {
                CanonicalProtobuf.bytes(output, 2, amount(dimension, 0));
            }
            CanonicalProtobuf.bytes(output, 3, new byte[32]);
        });
        assertThrows(IllegalArgumentException.class, () -> CapacityVectorV1.decode(unordered));
    }

    @Test
    void grantBindsKindIdentitySourceVersionVectorAndDigest() {
        final CapacityVectorV1 vector = CapacityVectorV1.empty();
        final byte[] grantId = Bytes.sha256(Bytes.utf8("grant"));
        final CapacityGrantV1 grant = new CapacityGrantV1(CapacityGrantKindV1.NON_OUTCOME_CONTROL,
                grantId, 42, vector);

        assertEquals(grant, CapacityGrantV1.decode(grant.canonicalBytes()));
        final byte[] tampered = grant.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> CapacityGrantV1.decode(tampered));
        assertThrows(IllegalArgumentException.class,
                () -> new CapacityGrantV1(CapacityGrantKindV1.OUTCOME_RESERVE, new byte[32], 1, vector));
        assertThrows(IllegalArgumentException.class,
                () -> CapacityGrantV1.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint32(output, 1, 1);
                    CanonicalProtobuf.bytes(output, 2, new byte[32]);
                    CanonicalProtobuf.uint64(output, 3, 1);
                    CanonicalProtobuf.bytes(output, 4, vector.canonicalBytes());
                    CanonicalProtobuf.bytes(output, 5, new byte[32]);
                })));
    }

    @Test
    void capacityGrantPreservesCompleteUnsigned64BitSourceVersion() {
        final CapacityGrantV1 grant = new CapacityGrantV1(CapacityGrantKindV1.NON_OUTCOME_CONTROL,
                Bytes.sha256(Bytes.utf8("high-bit-grant")), Long.MIN_VALUE, CapacityVectorV1.empty());

        final CapacityGrantV1 decoded = CapacityGrantV1.decode(grant.canonicalBytes());
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

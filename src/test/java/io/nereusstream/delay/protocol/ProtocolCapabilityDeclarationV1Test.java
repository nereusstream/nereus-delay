package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolCapabilityDeclarationV1Test {
    @Test
    void roundTripsCanonicalWorkerCapabilityAndSortsTuples() {
        final ProtocolTupleV1 oldTuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1);
        final ProtocolTupleV1 newTuple = new ProtocolTupleV1(1, 2, ProtocolTupleV1.CLIENT_COMMAND, 1, 1);
        final ProtocolCapabilityDeclarationV1 declaration = new ProtocolCapabilityDeclarationV1(
                "worker-b", bytes(32, 1), List.of(newTuple, oldTuple), 7, bytes(32, 2));

        final ProtocolCapabilityDeclarationV1 decoded =
                ProtocolCapabilityDeclarationV1.decode(declaration.canonicalBytes());

        assertEquals(declaration, decoded);
        assertEquals(List.of(oldTuple, newTuple), decoded.supportedTuples());
        assertTrueSupports(decoded, newTuple);
        assertArrayEquals(declaration.declarationDigest(), decoded.declarationDigest());
    }

    @Test
    void rejectsDuplicateTuplesAndInvalidEpoch() {
        final ProtocolTupleV1 tuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> new ProtocolCapabilityDeclarationV1(
                "worker", bytes(32, 1), List.of(tuple, tuple), 1, bytes(32, 2)));
        assertThrows(IllegalArgumentException.class, () -> new ProtocolCapabilityDeclarationV1(
                "worker", bytes(32, 1), List.of(tuple), 0, bytes(32, 2)));
    }

    private static void assertTrueSupports(final ProtocolCapabilityDeclarationV1 declaration,
                                           final ProtocolTupleV1 tuple) {
        if (!declaration.supports(tuple)) {
            throw new AssertionError("declaration did not advertise the requested tuple");
        }
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }
}

package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtocolCapabilityDeclarationTest {
    @Test
    void roundTripsCanonicalWorkerCapabilityAndSortsTuples() {
        final ProtocolTuple oldTuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1);
        final ProtocolTuple newTuple = new ProtocolTuple(1, 2, ProtocolTuple.CLIENT_COMMAND, 1, 1);
        final ProtocolCapabilityDeclaration declaration = new ProtocolCapabilityDeclaration(
                "worker-b", bytes(32, 1), List.of(newTuple, oldTuple), 7, bytes(32, 2));

        final ProtocolCapabilityDeclaration decoded =
                ProtocolCapabilityDeclaration.decode(declaration.canonicalBytes());

        assertEquals(declaration, decoded);
        assertEquals(List.of(oldTuple, newTuple), decoded.supportedTuples());
        assertTrueSupports(decoded, newTuple);
        assertArrayEquals(declaration.declarationDigest(), decoded.declarationDigest());
    }

    @Test
    void rejectsDuplicateTuplesAndInvalidEpoch() {
        final ProtocolTuple tuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProtocolCapabilityDeclaration(
                        "worker", bytes(32, 1), List.of(tuple, tuple), 1, bytes(32, 2)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ProtocolCapabilityDeclaration("worker", bytes(32, 1), List.of(tuple), 0, bytes(32, 2)));
    }

    private static void assertTrueSupports(final ProtocolCapabilityDeclaration declaration, final ProtocolTuple tuple) {
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

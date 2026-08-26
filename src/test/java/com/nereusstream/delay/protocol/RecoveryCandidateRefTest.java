package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class RecoveryCandidateRefTest {
    @Test
    void roundTripsBothCandidateBranches() {
        final RecoveryCandidateRef local = new RecoveryCandidateRef(
                RecoveryCandidateKind.LOCAL_STORE, bytes(16, 1), bytes(16, 2), bytes(32, 3), bytes(16, 4));
        final RecoveryCandidateRef catalog = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT, bytes(16, 1), bytes(16, 2), bytes(32, 3), null);

        assertEquals(local, RecoveryCandidateRef.decode(local.canonicalBytes()));
        assertEquals(catalog, RecoveryCandidateRef.decode(catalog.canonicalBytes()));
    }

    @Test
    void enforcesBranchAndDigestRules() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryCandidateRef(
                        RecoveryCandidateKind.LOCAL_STORE, bytes(16, 1), bytes(16, 2), bytes(32, 3), null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryCandidateRef(
                        RecoveryCandidateKind.CATALOG_CHECKPOINT,
                        bytes(16, 1),
                        bytes(16, 2),
                        bytes(32, 3),
                        bytes(16, 4)));

        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT, bytes(16, 1), bytes(16, 2), bytes(32, 3), null);
        final byte[] tampered = candidate.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryCandidateRef.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}

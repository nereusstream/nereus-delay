package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class RecoveryCandidateRefV1Test {
    @Test
    void roundTripsBothCandidateBranches() {
        final RecoveryCandidateRefV1 local = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.LOCAL_STORE, bytes(16, 1), bytes(16, 2), bytes(32, 3), bytes(16, 4));
        final RecoveryCandidateRefV1 catalog = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, bytes(16, 1), bytes(16, 2), bytes(32, 3), null);

        assertEquals(local, RecoveryCandidateRefV1.decode(local.canonicalBytes()));
        assertEquals(catalog, RecoveryCandidateRefV1.decode(catalog.canonicalBytes()));
    }

    @Test
    void enforcesBranchAndDigestRules() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryCandidateRefV1(
                        RecoveryCandidateKindV1.LOCAL_STORE, bytes(16, 1), bytes(16, 2), bytes(32, 3), null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryCandidateRefV1(
                        RecoveryCandidateKindV1.CATALOG_CHECKPOINT,
                        bytes(16, 1),
                        bytes(16, 2),
                        bytes(32, 3),
                        bytes(16, 4)));

        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, bytes(16, 1), bytes(16, 2), bytes(32, 3), null);
        final byte[] tampered = candidate.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryCandidateRefV1.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}

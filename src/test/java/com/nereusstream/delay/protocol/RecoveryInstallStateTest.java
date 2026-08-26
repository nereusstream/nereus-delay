package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class RecoveryInstallStateTest {
    @Test
    void roundTripsEveryInstallPhase() {
        for (RecoveryInstallPhase phase : RecoveryInstallPhase.values()) {
            final RecoveryInstallState state = new RecoveryInstallState(
                    phase,
                    bytes(16, phase.wireValue()),
                    phase == RecoveryInstallPhase.FRESH ? null : bytes(16, phase.wireValue() + 10));
            assertEquals(state, RecoveryInstallState.decode(state.canonicalBytes()));
        }
    }

    @Test
    void rejectsUnknownPhaseAndDigestDrift() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecoveryInstallState(RecoveryInstallPhase.OPEN, new byte[16], null));
        final RecoveryInstallState state =
                new RecoveryInstallState(RecoveryInstallPhase.OPEN, bytes(16, 1), bytes(16, 2));
        final byte[] tampered = state.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryInstallState.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}

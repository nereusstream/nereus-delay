package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoveryInstallStateV1Test {
    @Test
    void roundTripsEveryInstallPhase() {
        for (RecoveryInstallPhaseV1 phase : RecoveryInstallPhaseV1.values()) {
            final RecoveryInstallStateV1 state = new RecoveryInstallStateV1(phase, bytes(16, phase.wireValue()),
                    phase == RecoveryInstallPhaseV1.FRESH ? null : bytes(16, phase.wireValue() + 10));
            assertEquals(state, RecoveryInstallStateV1.decode(state.canonicalBytes()));
        }
    }

    @Test
    void rejectsUnknownPhaseAndDigestDrift() {
        assertThrows(IllegalArgumentException.class,
                () -> new RecoveryInstallStateV1(RecoveryInstallPhaseV1.OPEN, new byte[16], null));
        final RecoveryInstallStateV1 state = new RecoveryInstallStateV1(RecoveryInstallPhaseV1.OPEN,
                bytes(16, 1), bytes(16, 2));
        final byte[] tampered = state.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryInstallStateV1.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}

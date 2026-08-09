package io.nereusstream.delay.protocol;

/** Closed local install/open phases persisted in meta/RECOVERY. */
public enum RecoveryInstallPhaseV1 {
    FRESH(1),
    STAGED(2),
    INSTALLED(3),
    OPEN(4),
    CLOSED_CLEAN(5);

    private final int wireValue;

    RecoveryInstallPhaseV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RecoveryInstallPhaseV1 fromWire(final long value) {
        for (RecoveryInstallPhaseV1 phase : values()) {
            if (phase.wireValue == value) {
                return phase;
            }
        }
        throw new IllegalArgumentException("unknown RecoveryInstallPhaseV1: " + value);
    }
}

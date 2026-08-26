package com.nereusstream.delay.protocol;

/** Closed local install/open phases persisted in meta/RECOVERY. */
public enum RecoveryInstallPhase {
    FRESH(1),
    STAGED(2),
    INSTALLED(3),
    OPEN(4),
    CLOSED_CLEAN(5);

    private final int wireValue;

    RecoveryInstallPhase(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RecoveryInstallPhase fromWire(final long value) {
        for (RecoveryInstallPhase phase : values()) {
            if (phase.wireValue == value) {
                return phase;
            }
        }
        throw new IllegalArgumentException("unknown RecoveryInstallPhase: " + value);
    }
}

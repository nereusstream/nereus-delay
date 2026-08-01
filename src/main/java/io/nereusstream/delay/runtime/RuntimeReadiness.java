package io.nereusstream.delay.runtime;

public enum RuntimeReadiness {
    RECOVERING_EVIDENCE(1),
    READY(2),
    BLOCKED(3);

    private final int wireValue;

    RuntimeReadiness(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RuntimeReadiness fromWire(final int value) {
        for (RuntimeReadiness readiness : values()) {
            if (readiness.wireValue == value) {
                return readiness;
            }
        }
        throw new IllegalArgumentException("unknown runtime readiness: " + value);
    }
}


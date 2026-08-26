package com.nereusstream.delay.protocol;

/** Closed capacity-grant component registry. */
public enum CapacityGrantKind {
    OUTCOME_RESERVE(1),
    NON_OUTCOME_CONTROL(2),
    RECOVERY_WORKING(3),
    EMERGENCY_HEADROOM(4);

    private final int wireValue;

    CapacityGrantKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CapacityGrantKind fromWire(final long value) {
        for (CapacityGrantKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown CapacityGrantKind: " + value);
    }
}

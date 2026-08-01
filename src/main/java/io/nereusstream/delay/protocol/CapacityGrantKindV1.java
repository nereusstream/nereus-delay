package io.nereusstream.delay.protocol;

/** Closed V1 capacity-grant component registry. */
public enum CapacityGrantKindV1 {
    OUTCOME_RESERVE(1),
    NON_OUTCOME_CONTROL(2),
    RECOVERY_WORKING(3),
    EMERGENCY_HEADROOM(4);

    private final int wireValue;

    CapacityGrantKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CapacityGrantKindV1 fromWire(final long value) {
        for (CapacityGrantKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown CapacityGrantKindV1: " + value);
    }
}

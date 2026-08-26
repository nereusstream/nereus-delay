package com.nereusstream.delay.protocol;

/** Registry gate values used by public Lane control results. */
public enum LaneAdmissionGate {
    OPEN(1),
    ADMIN_PAUSED(2),
    ORDERING_BROKEN(3),
    CLOSED(4),
    RETIRED(5);

    private final int wireValue;

    LaneAdmissionGate(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static LaneAdmissionGate fromWire(final long value) {
        for (LaneAdmissionGate gate : values()) {
            if (gate.wireValue == value) {
                return gate;
            }
        }
        throw new IllegalArgumentException("unknown LaneAdmissionGate: " + value);
    }
}

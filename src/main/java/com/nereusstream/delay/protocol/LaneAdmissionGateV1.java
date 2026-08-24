package com.nereusstream.delay.protocol;

/** Registry gate values used by public Lane control results. */
public enum LaneAdmissionGateV1 {
    OPEN(1),
    ADMIN_PAUSED(2),
    ORDERING_BROKEN(3),
    CLOSED(4),
    RETIRED(5);

    private final int wireValue;

    LaneAdmissionGateV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static LaneAdmissionGateV1 fromWire(final long value) {
        for (LaneAdmissionGateV1 gate : values()) {
            if (gate.wireValue == value) {
                return gate;
            }
        }
        throw new IllegalArgumentException("unknown LaneAdmissionGateV1: " + value);
    }
}

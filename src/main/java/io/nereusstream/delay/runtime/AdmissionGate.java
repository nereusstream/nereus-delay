package io.nereusstream.delay.runtime;

public enum AdmissionGate {
    ABSENT(0),
    OPEN(1),
    ADMIN_PAUSED(2),
    ORDERING_BROKEN(3),
    CLOSED(4),
    RETIRED(5);

    private final int wireValue;

    AdmissionGate(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static AdmissionGate fromWire(final int value) {
        for (AdmissionGate gate : values()) {
            if (gate.wireValue == value) {
                return gate;
            }
        }
        throw new IllegalArgumentException("unknown admission gate: " + value);
    }
}


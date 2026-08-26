package com.nereusstream.delay.protocol;

/** Source kind for a bounded SLO time interval. */
public enum SloTimeEndpointKind {
    SEMANTIC_FIXED_EPOCH(1),
    BROKER_PERSISTENCE(2),
    TRUSTED_OBSERVATION(3);

    private final int wireValue;

    SloTimeEndpointKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloTimeEndpointKind fromWire(final long value) {
        for (SloTimeEndpointKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown SloTimeEndpointKind: " + value);
    }
}

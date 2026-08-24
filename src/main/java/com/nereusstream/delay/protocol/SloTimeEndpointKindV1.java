package com.nereusstream.delay.protocol;

/** Source kind for a bounded SLO time interval. */
public enum SloTimeEndpointKindV1 {
    SEMANTIC_FIXED_EPOCH(1),
    BROKER_PERSISTENCE(2),
    TRUSTED_OBSERVATION(3);

    private final int wireValue;

    SloTimeEndpointKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloTimeEndpointKindV1 fromWire(final long value) {
        for (SloTimeEndpointKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown SloTimeEndpointKindV1: " + value);
    }
}

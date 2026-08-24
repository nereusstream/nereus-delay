package com.nereusstream.delay.protocol;

/** Closed Profile registration/acceptance projection. */
public enum ProfileAcceptanceV1 {
    ABSENT(1),
    ACTIVE_FOR_FIRST_BINDING(2),
    CLOSED_FOR_FIRST_BINDING(3);

    private final int wireValue;

    ProfileAcceptanceV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ProfileAcceptanceV1 fromWire(final long value) {
        for (ProfileAcceptanceV1 acceptance : values()) {
            if (acceptance.wireValue == value) {
                return acceptance;
            }
        }
        throw new IllegalArgumentException("unknown ProfileAcceptanceV1: " + value);
    }
}

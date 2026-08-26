package com.nereusstream.delay.protocol;

/** Closed Profile registration/acceptance projection. */
public enum ProfileAcceptance {
    ABSENT(1),
    ACTIVE_FOR_FIRST_BINDING(2),
    CLOSED_FOR_FIRST_BINDING(3);

    private final int wireValue;

    ProfileAcceptance(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ProfileAcceptance fromWire(final long value) {
        for (ProfileAcceptance acceptance : values()) {
            if (acceptance.wireValue == value) {
                return acceptance;
            }
        }
        throw new IllegalArgumentException("unknown ProfileAcceptance: " + value);
    }
}

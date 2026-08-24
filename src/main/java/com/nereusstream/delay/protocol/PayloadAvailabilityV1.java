package com.nereusstream.delay.protocol;

/** Closed public payload availability categories. */
public enum PayloadAvailabilityV1 {
    UPLOAD_PENDING(1),
    INLINE_RETAINED(2),
    OBJECT_RETAINED(3),
    PAYLOAD_RECLAIMED(4),
    NOT_APPLICABLE(5);

    private final int wireValue;

    PayloadAvailabilityV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadAvailabilityV1 fromWire(final long value) {
        for (PayloadAvailabilityV1 availability : values()) {
            if (availability.wireValue == value) {
                return availability;
            }
        }
        throw new IllegalArgumentException("unknown PayloadAvailabilityV1: " + value);
    }
}

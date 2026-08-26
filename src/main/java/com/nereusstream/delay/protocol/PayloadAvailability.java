package com.nereusstream.delay.protocol;

/** Closed public payload availability categories. */
public enum PayloadAvailability {
    UPLOAD_PENDING(1),
    INLINE_RETAINED(2),
    OBJECT_RETAINED(3),
    PAYLOAD_RECLAIMED(4),
    NOT_APPLICABLE(5);

    private final int wireValue;

    PayloadAvailability(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadAvailability fromWire(final long value) {
        for (PayloadAvailability availability : values()) {
            if (availability.wireValue == value) {
                return availability;
            }
        }
        throw new IllegalArgumentException("unknown PayloadAvailability: " + value);
    }
}

package com.nereusstream.delay.runtime;

/** Source-ordered lifecycle of a large-payload reservation. */
public enum PayloadReservationStatus {
    RESERVED(1),
    COMMITTED(2),
    ABANDONED(3),
    EXPIRED(4);

    private final int wireValue;

    PayloadReservationStatus(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadReservationStatus fromWire(final int wireValue) {
        for (PayloadReservationStatus status : values()) {
            if (status.wireValue == wireValue) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown payload reservation status: " + wireValue);
    }
}

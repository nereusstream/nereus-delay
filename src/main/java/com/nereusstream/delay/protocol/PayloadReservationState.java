package com.nereusstream.delay.protocol;

/** Closed public payload reservation states. */
public enum PayloadReservationState {
    PAYLOAD_RESERVED(1),
    COMMITTED(2),
    ABANDONED(3),
    RESERVATION_EXPIRED(4);

    private final int wireValue;

    PayloadReservationState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadReservationState fromWire(final long value) {
        for (PayloadReservationState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown PayloadReservationState: " + value);
    }
}

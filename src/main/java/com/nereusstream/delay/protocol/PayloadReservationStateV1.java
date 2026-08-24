package com.nereusstream.delay.protocol;

/** Closed public payload reservation states. */
public enum PayloadReservationStateV1 {
    PAYLOAD_RESERVED(1),
    COMMITTED(2),
    ABANDONED(3),
    RESERVATION_EXPIRED(4);

    private final int wireValue;

    PayloadReservationStateV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadReservationStateV1 fromWire(final long value) {
        for (PayloadReservationStateV1 state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown PayloadReservationStateV1: " + value);
    }
}

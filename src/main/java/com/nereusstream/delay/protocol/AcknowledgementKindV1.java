package com.nereusstream.delay.protocol;

/** Registry acknowledgement categories used by irreversible controls. */
public enum AcknowledgementKindV1 {
    POSSIBLE_DUPLICATE(1),
    POSSIBLE_DELIVERY(2),
    ORDER_LOSS(3);

    private final int wireValue;

    AcknowledgementKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static AcknowledgementKindV1 fromWire(final long value) {
        for (AcknowledgementKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown AcknowledgementKindV1: " + value);
    }
}

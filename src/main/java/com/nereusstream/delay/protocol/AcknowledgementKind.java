package com.nereusstream.delay.protocol;

/** Registry acknowledgement categories used by irreversible controls. */
public enum AcknowledgementKind {
    POSSIBLE_DUPLICATE(1),
    POSSIBLE_DELIVERY(2),
    ORDER_LOSS(3);

    private final int wireValue;

    AcknowledgementKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static AcknowledgementKind fromWire(final long value) {
        for (AcknowledgementKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown AcknowledgementKind: " + value);
    }
}

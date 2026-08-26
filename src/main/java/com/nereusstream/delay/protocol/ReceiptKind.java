package com.nereusstream.delay.protocol;

/** Closed receipt payload kinds carried by an {@link ReceiptFrame}. */
public enum ReceiptKind {
    COMMAND_QUEUED(1),
    COMMAND_APPLIED(2),
    NATIVE_DELIVERY(3),
    PAYLOAD_RESERVATION(4),
    CONTROL_OPERATION(5);

    private final int wireValue;

    ReceiptKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ReceiptKind fromWire(final int value) {
        for (ReceiptKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown receipt kind: " + value);
    }
}

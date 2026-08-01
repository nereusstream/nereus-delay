package io.nereusstream.delay.protocol;

/** Closed public DLQ export projection states. */
public enum DlqExportStateV1 {
    NOT_CONFIGURED(1),
    PENDING(2),
    PUBLISHED(3),
    UNCERTAIN(4),
    FAILED_PERMANENT(5);

    private final int wireValue;

    DlqExportStateV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static DlqExportStateV1 fromWire(final long value) {
        for (DlqExportStateV1 state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown DlqExportStateV1: " + value);
    }
}

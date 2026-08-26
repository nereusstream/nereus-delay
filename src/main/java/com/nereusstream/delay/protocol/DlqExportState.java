package com.nereusstream.delay.protocol;

/** Closed public DLQ export projection states. */
public enum DlqExportState {
    NOT_CONFIGURED(1),
    PENDING(2),
    PUBLISHED(3),
    UNCERTAIN(4),
    FAILED_PERMANENT(5);

    private final int wireValue;

    DlqExportState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static DlqExportState fromWire(final long value) {
        for (DlqExportState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown DlqExportState: " + value);
    }
}

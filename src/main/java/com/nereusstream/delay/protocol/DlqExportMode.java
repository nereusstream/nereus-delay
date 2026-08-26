package com.nereusstream.delay.protocol;

/** Closed policy for the terminal DLQ export outbox. */
public enum DlqExportMode {
    NOT_CONFIGURED(1),
    BASELINE_AT_LEAST_ONCE(2);

    private final int wireValue;

    DlqExportMode(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static DlqExportMode fromWire(final long value) {
        for (DlqExportMode mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown DlqExportMode: " + value);
    }
}

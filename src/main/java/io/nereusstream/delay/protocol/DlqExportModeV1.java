package io.nereusstream.delay.protocol;

/** Closed policy for the terminal DLQ export outbox. */
public enum DlqExportModeV1 {
    NOT_CONFIGURED(1),
    BASELINE_AT_LEAST_ONCE(2);

    private final int wireValue;

    DlqExportModeV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static DlqExportModeV1 fromWire(final long value) {
        for (DlqExportModeV1 mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown DlqExportModeV1: " + value);
    }
}

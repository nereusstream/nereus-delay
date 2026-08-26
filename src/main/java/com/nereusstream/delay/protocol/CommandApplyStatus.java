package com.nereusstream.delay.protocol;

/** Closed public command application status. */
public enum CommandApplyStatus {
    APPLIED(1),
    REJECTED(2);

    private final int wireValue;

    CommandApplyStatus(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CommandApplyStatus fromWire(final long value) {
        for (CommandApplyStatus status : values()) {
            if (status.wireValue == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown CommandApplyStatus: " + value);
    }
}

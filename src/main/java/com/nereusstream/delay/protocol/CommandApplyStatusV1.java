package com.nereusstream.delay.protocol;

/** Closed public command application status. */
public enum CommandApplyStatusV1 {
    APPLIED(1),
    REJECTED(2);

    private final int wireValue;

    CommandApplyStatusV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CommandApplyStatusV1 fromWire(final long value) {
        for (CommandApplyStatusV1 status : values()) {
            if (status.wireValue == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown CommandApplyStatusV1: " + value);
    }
}

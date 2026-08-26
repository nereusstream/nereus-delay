package com.nereusstream.delay.protocol;

/** Registry policy for the close-lane control branch. */
public enum ClosePolicy {
    _FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED(1);

    private final int wireValue;

    ClosePolicy(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ClosePolicy fromWire(final long value) {
        for (ClosePolicy policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown ClosePolicy: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Registry policy for the V1 close-lane control branch. */
public enum ClosePolicyV1 {
    V1_FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED(1);

    private final int wireValue;

    ClosePolicyV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ClosePolicyV1 fromWire(final long value) {
        for (ClosePolicyV1 policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown ClosePolicyV1: " + value);
    }
}

package com.nereusstream.delay.runtime;

/** Registry {@code UncertainRetryAuthority} values. */
public enum UncertainRetryAuthority {
    NONE(1),
    PINNED_POLICY(2),
    CONTROL_OVERRIDE(3);

    private final int wireValue;

    UncertainRetryAuthority(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UncertainRetryAuthority fromWire(final long value) {
        for (UncertainRetryAuthority authority : values()) {
            if (authority.wireValue == value) {
                return authority;
            }
        }
        throw new IllegalArgumentException("unknown uncertain retry authority: " + value);
    }
}

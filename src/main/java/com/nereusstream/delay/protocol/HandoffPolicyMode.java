package com.nereusstream.delay.protocol;

/** Runtime mode of the signed bounded handoff policy. */
public enum HandoffPolicyMode {
    DISABLED(1),
    SHADOW(2),
    ENABLED(3);

    private final int wireValue;

    HandoffPolicyMode(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static HandoffPolicyMode fromWire(final long value) {
        for (HandoffPolicyMode mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown HandoffPolicyMode: " + value);
    }
}

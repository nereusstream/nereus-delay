package com.nereusstream.delay.protocol;

/** Explicit per-message permission for the Pulsar native delivery paths. */
public enum NativeDeliveryPolicy {
    FORBID(1),
    ALLOW_MANAGED_HANDOFF(2),
    ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF(3);

    private final int wireValue;

    NativeDeliveryPolicy(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public boolean allowsManagedHandoff() {
        return this != FORBID;
    }

    public boolean allowsAutoFast() {
        return this == ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF;
    }

    public static NativeDeliveryPolicy fromWire(final long value) {
        for (NativeDeliveryPolicy policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown NativeDeliveryPolicy: " + value);
    }
}

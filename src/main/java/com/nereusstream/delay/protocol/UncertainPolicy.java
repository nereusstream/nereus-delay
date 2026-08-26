package com.nereusstream.delay.protocol;

/** Closed policy for an unresolved destination attempt. */
public enum UncertainPolicy {
    HOLD_FOR_EVIDENCE(1),
    BOUNDED_RETRY_POSSIBLE_DUPLICATE(2),
    BOUNDED_TERMINAL_POSSIBLE_DELIVERY(3);

    private final int wireValue;

    UncertainPolicy(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UncertainPolicy fromWire(final long value) {
        for (UncertainPolicy policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown UncertainPolicy: " + value);
    }
}

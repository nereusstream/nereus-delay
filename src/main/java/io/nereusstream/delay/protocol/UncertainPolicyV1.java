package io.nereusstream.delay.protocol;

/** Closed policy for an unresolved destination attempt. */
public enum UncertainPolicyV1 {
    HOLD_FOR_EVIDENCE(1),
    BOUNDED_RETRY_POSSIBLE_DUPLICATE(2),
    BOUNDED_TERMINAL_POSSIBLE_DELIVERY(3);

    private final int wireValue;

    UncertainPolicyV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UncertainPolicyV1 fromWire(final long value) {
        for (UncertainPolicyV1 policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown UncertainPolicyV1: " + value);
    }
}

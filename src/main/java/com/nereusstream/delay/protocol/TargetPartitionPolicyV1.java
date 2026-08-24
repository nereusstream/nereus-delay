package com.nereusstream.delay.protocol;

/** Closed target-partition selection policy from the V1 Profile registry. */
public enum TargetPartitionPolicyV1 {
    EXPLICIT_ONLY(1),
    HASH_ONLY(2),
    EXPLICIT_OR_HASH(3);

    private final int wireValue;

    TargetPartitionPolicyV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static TargetPartitionPolicyV1 fromWire(final long value) {
        for (TargetPartitionPolicyV1 policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown TargetPartitionPolicyV1: " + value);
    }
}

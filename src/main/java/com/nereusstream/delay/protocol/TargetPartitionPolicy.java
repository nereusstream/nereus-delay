package com.nereusstream.delay.protocol;

/** Closed target-partition selection policy from the Profile registry. */
public enum TargetPartitionPolicy {
    EXPLICIT_ONLY(1),
    HASH_ONLY(2),
    EXPLICIT_OR_HASH(3);

    private final int wireValue;

    TargetPartitionPolicy(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static TargetPartitionPolicy fromWire(final long value) {
        for (TargetPartitionPolicy policy : values()) {
            if (policy.wireValue == value) {
                return policy;
            }
        }
        throw new IllegalArgumentException("unknown TargetPartitionPolicy: " + value);
    }
}

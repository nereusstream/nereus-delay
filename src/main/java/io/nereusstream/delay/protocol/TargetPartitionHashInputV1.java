package io.nereusstream.delay.protocol;

/** Closed input selection for Profile target-partition hashing. */
public enum TargetPartitionHashInputV1 {
    ORDERING_KEY(1),
    ADAPTER_MESSAGE_KEY(2),
    DELAY_MESSAGE_ID(3);

    private final int wireValue;

    TargetPartitionHashInputV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static TargetPartitionHashInputV1 fromWire(final long value) {
        for (TargetPartitionHashInputV1 input : values()) {
            if (input.wireValue == value) {
                return input;
            }
        }
        throw new IllegalArgumentException("unknown TargetPartitionHashInputV1: " + value);
    }
}

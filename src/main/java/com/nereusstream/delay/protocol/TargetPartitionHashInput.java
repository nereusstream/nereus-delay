package com.nereusstream.delay.protocol;

/** Closed input selection for Profile target-partition hashing. */
public enum TargetPartitionHashInput {
    ORDERING_KEY(1),
    ADAPTER_MESSAGE_KEY(2),
    DELAY_MESSAGE_ID(3);

    private final int wireValue;

    TargetPartitionHashInput(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static TargetPartitionHashInput fromWire(final long value) {
        for (TargetPartitionHashInput input : values()) {
            if (input.wireValue == value) {
                return input;
            }
        }
        throw new IllegalArgumentException("unknown TargetPartitionHashInput: " + value);
    }
}

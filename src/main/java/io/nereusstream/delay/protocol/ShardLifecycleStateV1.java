package io.nereusstream.delay.protocol;

/** Closed public Shard lifecycle values. */
public enum ShardLifecycleStateV1 {
    UNASSIGNED(1),
    ACQUIRING(2),
    RESTORING(3),
    CATCHING_UP(4),
    ACTIVE_FOR_COMMANDS(5),
    DRAINING(6),
    FENCED(7),
    FAILED(8);

    private final int wireValue;

    ShardLifecycleStateV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ShardLifecycleStateV1 fromWire(final long value) {
        for (ShardLifecycleStateV1 state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown ShardLifecycleStateV1: " + value);
    }
}

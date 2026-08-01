package io.nereusstream.delay.ownership;

public enum ShardLifecycleState {
    UNASSIGNED(1),
    ACQUIRING(2),
    RESTORING(3),
    CATCHING_UP(4),
    ACTIVE_FOR_COMMANDS(5),
    DRAINING(6),
    FENCED(7),
    FAILED(8);

    private final int wireValue;

    ShardLifecycleState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}


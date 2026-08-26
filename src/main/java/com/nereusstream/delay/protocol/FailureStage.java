package com.nereusstream.delay.protocol;

/** Closed public stage projection for stable errors. */
public enum FailureStage {
    PREPARATION(1),
    ENQUEUE(2),
    APPLICATION(3),
    QUERY(4),
    PAYLOAD(5),
    CONTROL(6),
    PUBLISH(7),
    RECOVERY(8),
    INTEGRITY(9);

    private final int wireValue;

    FailureStage(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static FailureStage fromWire(final long value) {
        for (FailureStage stage : values()) {
            if (stage.wireValue == value) {
                return stage;
            }
        }
        throw new IllegalArgumentException("unknown FailureStage: " + value);
    }
}

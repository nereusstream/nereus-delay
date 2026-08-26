package com.nereusstream.delay.protocol;

/** Closed target kinds accepted by a prepared Control Operation. */
public enum ControlTargetKind {
    SHARD(1),
    LANE(2),
    MESSAGE(3),
    ROUTE(4),
    PROFILE(5),
    QUOTA_GRANT(6);

    private final int wireValue;

    ControlTargetKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlTargetKind fromWire(final long value) {
        for (ControlTargetKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlTargetKind: " + value);
    }
}

package io.nereusstream.delay.protocol;

/** Closed target kinds accepted by a prepared Control Operation. */
public enum ControlTargetKindV1 {
    SHARD(1),
    LANE(2),
    MESSAGE(3),
    ROUTE(4),
    PROFILE(5),
    QUOTA_GRANT(6);

    private final int wireValue;

    ControlTargetKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlTargetKindV1 fromWire(final long value) {
        for (ControlTargetKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlTargetKindV1: " + value);
    }
}

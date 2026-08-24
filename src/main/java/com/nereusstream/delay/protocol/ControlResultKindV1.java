package com.nereusstream.delay.protocol;

/** Closed typed-result branch tags for Control Operation queries. */
public enum ControlResultKindV1 {
    LANE(1),
    SHARD(2),
    CHECKPOINT(3),
    PROFILE(4),
    QUOTA(5),
    MESSAGE(6),
    CHECKPOINT_CATALOG(7),
    ROUTE(8),
    SECRET_ROTATION(9);

    private final int wireValue;

    ControlResultKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlResultKindV1 fromWire(final int value) {
        for (ControlResultKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlResultKindV1: " + value);
    }
}

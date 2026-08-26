package com.nereusstream.delay.protocol;

/** Closed typed-result branch tags for Control Operation queries. */
public enum ControlResultKind {
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

    ControlResultKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlResultKind fromWire(final int value) {
        for (ControlResultKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlResultKind: " + value);
    }
}

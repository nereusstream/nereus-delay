package com.nereusstream.delay.protocol;

/** The only routing hash admitted by the Registry. */
public enum RoutingHashVersion {
    ROUTING_HASH(1);

    private final int wireValue;

    RoutingHashVersion(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RoutingHashVersion fromWire(final long value) {
        for (RoutingHashVersion version : values()) {
            if (version.wireValue == value) {
                return version;
            }
        }
        throw new IllegalArgumentException("unknown RoutingHashVersion: " + value);
    }
}

package io.nereusstream.delay.protocol;

/** The only routing hash admitted by the V1 Registry. */
public enum RoutingHashVersionV1 {
    ROUTING_HASH_V1(1);

    private final int wireValue;

    RoutingHashVersionV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RoutingHashVersionV1 fromWire(final long value) {
        for (RoutingHashVersionV1 version : values()) {
            if (version.wireValue == value) {
                return version;
            }
        }
        throw new IllegalArgumentException("unknown RoutingHashVersionV1: " + value);
    }
}

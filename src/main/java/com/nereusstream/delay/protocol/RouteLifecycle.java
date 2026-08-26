package com.nereusstream.delay.protocol;

/** Closed route lifecycle values used by Route control results. */
public enum RouteLifecycle {
    ACTIVE_FOR_NEW(1),
    CONTROL_ONLY(2),
    DRAINING(3),
    RETIRED(4);

    private final int wireValue;

    RouteLifecycle(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RouteLifecycle fromWire(final long value) {
        for (RouteLifecycle lifecycle : values()) {
            if (lifecycle.wireValue == value) {
                return lifecycle;
            }
        }
        throw new IllegalArgumentException("unknown RouteLifecycle: " + value);
    }
}

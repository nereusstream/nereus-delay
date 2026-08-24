package com.nereusstream.delay.protocol;

/** Closed route lifecycle values used by Route control results. */
public enum RouteLifecycleV1 {
    ACTIVE_FOR_NEW(1),
    CONTROL_ONLY(2),
    DRAINING(3),
    RETIRED(4);

    private final int wireValue;

    RouteLifecycleV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RouteLifecycleV1 fromWire(final long value) {
        for (RouteLifecycleV1 lifecycle : values()) {
            if (lifecycle.wireValue == value) {
                return lifecycle;
            }
        }
        throw new IllegalArgumentException("unknown RouteLifecycleV1: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Closed reasons for an ALL_ACCEPTED due-admission companion sample. */
public enum DueExclusionReasonV1 {
    ADMIN_PAUSED(1),
    ORDERING_BROKEN(2),
    CLOSED(3),
    RECOVERING_EVIDENCE(4),
    CAPABILITY_BLOCKED(5),
    CLOCK_GATED(6),
    ORDER_HEAD_BLOCKED(7),
    CAPACITY_GATED(8),
    ADAPTER_LANE_FULL(9);

    private final int wireValue;

    DueExclusionReasonV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static DueExclusionReasonV1 fromWire(final long value) {
        for (DueExclusionReasonV1 reason : values()) {
            if (reason.wireValue == value) {
                return reason;
            }
        }
        throw new IllegalArgumentException("unknown DueExclusionReasonV1: " + value);
    }
}

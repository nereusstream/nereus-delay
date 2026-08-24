package com.nereusstream.delay.protocol;

/** Closed evidence status for a bare message identity query. */
public enum FirstScheduleEligibilityV1 {
    NOT_PROVEN(1),
    EXPIRED_BY_SOURCE_FENCE(2);

    private final int wireValue;

    FirstScheduleEligibilityV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static FirstScheduleEligibilityV1 fromWire(final long value) {
        for (FirstScheduleEligibilityV1 eligibility : values()) {
            if (eligibility.wireValue == value) {
                return eligibility;
            }
        }
        throw new IllegalArgumentException("unknown FirstScheduleEligibilityV1: " + value);
    }
}

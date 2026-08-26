package com.nereusstream.delay.protocol;

/** Closed evidence status for a bare message identity query. */
public enum FirstScheduleEligibility {
    NOT_PROVEN(1),
    EXPIRED_BY_SOURCE_FENCE(2);

    private final int wireValue;

    FirstScheduleEligibility(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static FirstScheduleEligibility fromWire(final long value) {
        for (FirstScheduleEligibility eligibility : values()) {
            if (eligibility.wireValue == value) {
                return eligibility;
            }
        }
        throw new IllegalArgumentException("unknown FirstScheduleEligibility: " + value);
    }
}

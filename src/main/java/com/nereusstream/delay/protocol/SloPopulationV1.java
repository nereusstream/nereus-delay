package com.nereusstream.delay.protocol;

/** Closed V1 SLO population registry. */
public enum SloPopulationV1 {
    HEALTHY(1),
    ALL_ACCEPTED(2);

    private final int wireValue;

    SloPopulationV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloPopulationV1 fromWire(final long value) {
        for (SloPopulationV1 population : values()) {
            if (population.wireValue == value) {
                return population;
            }
        }
        throw new IllegalArgumentException("unknown SloPopulationV1: " + value);
    }
}

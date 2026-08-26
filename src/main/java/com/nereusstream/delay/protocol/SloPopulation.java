package com.nereusstream.delay.protocol;

/** Closed SLO population registry. */
public enum SloPopulation {
    HEALTHY(1),
    ALL_ACCEPTED(2);

    private final int wireValue;

    SloPopulation(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloPopulation fromWire(final long value) {
        for (SloPopulation population : values()) {
            if (population.wireValue == value) {
                return population;
            }
        }
        throw new IllegalArgumentException("unknown SloPopulation: " + value);
    }
}

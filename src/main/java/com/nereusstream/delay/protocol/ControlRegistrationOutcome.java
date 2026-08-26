package com.nereusstream.delay.protocol;

/** Closed outcome kinds for Control Operation registration. */
public enum ControlRegistrationOutcome {
    RECORDED(1),
    DEFINITELY_NOT_RECORDED(2),
    RECORD_UNCERTAIN(3);

    private final int wireValue;

    ControlRegistrationOutcome(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlRegistrationOutcome fromWire(final long value) {
        for (ControlRegistrationOutcome outcome : values()) {
            if (outcome.wireValue == value) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("unknown ControlRegistrationOutcome: " + value);
    }
}

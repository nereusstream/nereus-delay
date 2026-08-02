package io.nereusstream.delay.protocol;

/** Closed outcome kinds for Control Operation registration. */
public enum ControlRegistrationOutcomeV1 {
    RECORDED(1),
    DEFINITELY_NOT_RECORDED(2),
    RECORD_UNCERTAIN(3);

    private final int wireValue;

    ControlRegistrationOutcomeV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlRegistrationOutcomeV1 fromWire(final long value) {
        for (ControlRegistrationOutcomeV1 outcome : values()) {
            if (outcome.wireValue == value) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("unknown ControlRegistrationOutcomeV1: " + value);
    }
}

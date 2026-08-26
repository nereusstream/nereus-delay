package com.nereusstream.delay.protocol;

/** Closed SLO final outcomes; bad evidence is never treated as success. */
public enum SloFinalOutcome {
    SUCCESS(1),
    BAD_DEFINITIVE(2),
    BAD_UNCERTAIN(3),
    BAD_TIMEOUT(4),
    BAD_UNQUALIFIED_TIME(5),
    BAD_EVIDENCE_GAP(6);

    private final int wireValue;

    SloFinalOutcome(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public boolean isBad() {
        return this != SUCCESS;
    }

    public static SloFinalOutcome fromWire(final long value) {
        for (SloFinalOutcome outcome : values()) {
            if (outcome.wireValue == value) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("unknown SloFinalOutcome: " + value);
    }
}

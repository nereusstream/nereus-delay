package io.nereusstream.delay.runtime;

/** Persisted open-attempt states from Protocol Registry §2.1. */
public enum AttemptLedgerState {
    PUBLISHING(1),
    UNCERTAIN(2);

    private final int wireValue;

    AttemptLedgerState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static AttemptLedgerState fromWire(final long value) {
        for (AttemptLedgerState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown attempt ledger state: " + value);
    }
}

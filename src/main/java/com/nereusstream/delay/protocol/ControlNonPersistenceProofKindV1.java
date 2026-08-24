package com.nereusstream.delay.protocol;

/** Closed proof kinds for a Control Operation that was not registered. */
public enum ControlNonPersistenceProofKindV1 {
    BEFORE_OXIA_OWNERSHIP(1),
    OXIA_CONDITIONAL_REJECTION(2);

    private final int wireValue;

    ControlNonPersistenceProofKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlNonPersistenceProofKindV1 fromWire(final long value) {
        for (ControlNonPersistenceProofKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlNonPersistenceProofKindV1: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Closed proof kinds for a Control Operation that was not registered. */
public enum ControlNonPersistenceProofKind {
    BEFORE_OXIA_OWNERSHIP(1),
    OXIA_CONDITIONAL_REJECTION(2);

    private final int wireValue;

    ControlNonPersistenceProofKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlNonPersistenceProofKind fromWire(final long value) {
        for (ControlNonPersistenceProofKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlNonPersistenceProofKind: " + value);
    }
}

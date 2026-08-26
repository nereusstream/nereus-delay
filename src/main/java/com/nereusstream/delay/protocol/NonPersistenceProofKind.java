package com.nereusstream.delay.protocol;

/** Closed proof kinds for a definitive non-persistence outcome. */
public enum NonPersistenceProofKind {
    LOCAL_BEFORE_PRODUCER_OWNERSHIP(1),
    KAFKA_DEFINITIVE_REJECTION(2),
    PULSAR_GUARD_REJECTION(3),
    LIBRARY_CERTIFIED_PRE_OWNERSHIP_CANCEL(4);

    private final int wireValue;

    NonPersistenceProofKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static NonPersistenceProofKind fromWire(final long value) {
        for (NonPersistenceProofKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown NonPersistenceProofKind: " + value);
    }
}

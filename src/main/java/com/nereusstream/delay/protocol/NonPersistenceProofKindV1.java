package com.nereusstream.delay.protocol;

/** Closed proof kinds for a definitive non-persistence outcome. */
public enum NonPersistenceProofKindV1 {
    LOCAL_BEFORE_PRODUCER_OWNERSHIP(1),
    KAFKA_DEFINITIVE_REJECTION(2),
    PULSAR_GUARD_REJECTION(3),
    LIBRARY_CERTIFIED_PRE_OWNERSHIP_CANCEL(4);

    private final int wireValue;

    NonPersistenceProofKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static NonPersistenceProofKindV1 fromWire(final long value) {
        for (NonPersistenceProofKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown NonPersistenceProofKindV1: " + value);
    }
}

package io.nereusstream.delay.protocol;

/** Closed recovery candidate reference kinds from Registry §13. */
public enum RecoveryCandidateKindV1 {
    LOCAL_STORE(1),
    CATALOG_CHECKPOINT(2);

    private final int wireValue;

    RecoveryCandidateKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RecoveryCandidateKindV1 fromWire(final long value) {
        for (RecoveryCandidateKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown RecoveryCandidateKindV1: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Closed recovery candidate reference kinds from Registry §13. */
public enum RecoveryCandidateKind {
    LOCAL_STORE(1),
    CATALOG_CHECKPOINT(2);

    private final int wireValue;

    RecoveryCandidateKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static RecoveryCandidateKind fromWire(final long value) {
        for (RecoveryCandidateKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown RecoveryCandidateKind: " + value);
    }
}

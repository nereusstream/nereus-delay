package com.nereusstream.delay.protocol;

/** Closed profile kind registry. */
public enum ProfileKind {
    DESTINATION(1),
    DELIVERY_CAPABILITY(2),
    OBJECT_STORE(3),
    EVIDENCE_VERIFIER(4);

    private final int wireValue;

    ProfileKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ProfileKind fromWire(final long value) {
        for (ProfileKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ProfileKind: " + value);
    }
}

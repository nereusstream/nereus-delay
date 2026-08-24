package com.nereusstream.delay.protocol;

/** Closed V1 profile kind registry. */
public enum ProfileKindV1 {
    DESTINATION(1),
    DELIVERY_CAPABILITY(2),
    OBJECT_STORE(3),
    EVIDENCE_VERIFIER(4);

    private final int wireValue;

    ProfileKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ProfileKindV1 fromWire(final long value) {
        for (ProfileKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ProfileKindV1: " + value);
    }
}

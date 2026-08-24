package com.nereusstream.delay.protocol;

/** Closed evidence verification states visible in terminal query views. */
public enum EvidenceVerificationStatusV1 {
    VERIFIED_PUBLISHED(1),
    VERIFIED_NOT_PUBLISHED(2),
    UNRESOLVED(3);

    private final int wireValue;

    EvidenceVerificationStatusV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static EvidenceVerificationStatusV1 fromWire(final long value) {
        for (EvidenceVerificationStatusV1 status : values()) {
            if (status.wireValue == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown EvidenceVerificationStatusV1: " + value);
    }
}

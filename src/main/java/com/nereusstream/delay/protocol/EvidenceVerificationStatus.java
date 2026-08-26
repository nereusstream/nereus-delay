package com.nereusstream.delay.protocol;

/** Closed evidence verification states visible in terminal query views. */
public enum EvidenceVerificationStatus {
    VERIFIED_PUBLISHED(1),
    VERIFIED_NOT_PUBLISHED(2),
    UNRESOLVED(3);

    private final int wireValue;

    EvidenceVerificationStatus(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static EvidenceVerificationStatus fromWire(final long value) {
        for (EvidenceVerificationStatus status : values()) {
            if (status.wireValue == value) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown EvidenceVerificationStatus: " + value);
    }
}

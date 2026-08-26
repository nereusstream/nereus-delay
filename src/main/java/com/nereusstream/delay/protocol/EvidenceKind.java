package com.nereusstream.delay.protocol;

/** Closed evidence cursor kinds. */
public enum EvidenceKind {
    KAFKA_RECEIPT_CONTIGUOUS(1),
    PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS(2);

    private final int wireValue;

    EvidenceKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static EvidenceKind fromWire(final long value) {
        for (EvidenceKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown EvidenceKind: " + value);
    }
}

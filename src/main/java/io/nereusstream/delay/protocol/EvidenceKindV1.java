package io.nereusstream.delay.protocol;

/** Closed evidence cursor kinds. */
public enum EvidenceKindV1 {
    KAFKA_RECEIPT_CONTIGUOUS(1),
    PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS(2);

    private final int wireValue;

    EvidenceKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static EvidenceKindV1 fromWire(final long value) {
        for (EvidenceKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown EvidenceKindV1: " + value);
    }
}

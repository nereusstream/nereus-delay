package com.nereusstream.delay.protocol;

/** Closed evidence types accepted by public evidence references. */
public enum PublishEvidenceKindV1 {
    KAFKA_PRODUCE_ACK(1),
    KAFKA_TRANSACTIONAL_RECEIPT(2),
    KAFKA_RECEIPT_ABSENCE(3),
    PULSAR_SEND_ACK(4),
    PULSAR_ATTEMPT_JOURNAL(5),
    PULSAR_JOURNAL_ABSENCE(6),
    BROKER_RESOURCE_GUARD_REJECTION(7),
    OPERATOR_ATTESTATION(8),
    ADAPTER_NON_SUBMISSION(9),
    BROKER_DEFINITIVE_REJECTION(10);

    private final int wireValue;

    PublishEvidenceKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PublishEvidenceKindV1 fromWire(final long value) {
        for (PublishEvidenceKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown PublishEvidenceKindV1: " + value);
    }
}

package io.nereusstream.delay.protocol;

/** Closed destination channel kinds registered by V1. */
public enum ChannelKindV1 {
    BASELINE_PRODUCER(1),
    KAFKA_TRANSACTIONAL_RECEIPT(2),
    PULSAR_DEDUP_PRODUCER(3),
    PULSAR_NATIVE_DELAYED(4),
    DLQ_EXPORT(5);

    private final int wireValue;

    ChannelKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public boolean requiresEvidenceResource() {
        return this == KAFKA_TRANSACTIONAL_RECEIPT || this == PULSAR_DEDUP_PRODUCER;
    }

    public static ChannelKindV1 fromWire(final long value) {
        for (ChannelKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ChannelKindV1: " + value);
    }
}

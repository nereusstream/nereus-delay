package com.nereusstream.delay.protocol;

/** Closed destination channel kinds registered by the current design. */
public enum ChannelKind {
    BASELINE_PRODUCER(1),
    KAFKA_TRANSACTIONAL_RECEIPT(2),
    PULSAR_DEDUP_PRODUCER(3),
    PULSAR_NATIVE_DELAYED(4),
    DLQ_EXPORT(5);

    private final int wireValue;

    ChannelKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public boolean requiresEvidenceResource() {
        return this == KAFKA_TRANSACTIONAL_RECEIPT || this == PULSAR_DEDUP_PRODUCER;
    }

    public static ChannelKind fromWire(final long value) {
        for (ChannelKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ChannelKind: " + value);
    }
}

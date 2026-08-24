package com.nereusstream.delay.protocol;

/** Closed V1 resource kinds accepted by resource-retirement mutations. */
public enum ResourceKind {
    PAYLOAD_OBJECT(1),
    CHECKPOINT(2),
    DLQ_EXPORT_OBJECT(3),
    KAFKA_RECEIPT_SLOT(4),
    PULSAR_JOURNAL_GENERATION(5),
    LANE_CHANNEL(6),
    LOCAL_STORE(7);

    private final int wireValue;

    ResourceKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ResourceKind fromWire(final long value) {
        for (ResourceKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown resource kind: " + value);
    }
}

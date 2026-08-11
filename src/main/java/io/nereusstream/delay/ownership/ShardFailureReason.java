package io.nereusstream.delay.ownership;

/** Closed V1 reason projection for a terminal FAILED shard lifecycle. */
public enum ShardFailureReason {
    NONE(0),
    SOURCE_GAP(1),
    STORE_CORRUPTION(2),
    CATALOG_OR_LINEAGE_INTEGRITY(3),
    UNSUPPORTED_ACTIVATED_PROTOCOL(4),
    CONTROL_PROTOCOL_INTEGRITY(5),
    UNRECOVERABLE_EVIDENCE_GAP(6);

    private final int wireValue;

    ShardFailureReason(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}

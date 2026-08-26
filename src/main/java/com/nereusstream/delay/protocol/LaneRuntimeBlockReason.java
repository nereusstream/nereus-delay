package com.nereusstream.delay.protocol;

/** Closed reason for a Lane whose runtime evidence is blocked. */
public enum LaneRuntimeBlockReason {
    CAPABILITY(1),
    CREDENTIAL_BINDING_DRIFT(2),
    DESTINATION_INCARNATION_MISMATCH(3),
    EVIDENCE_GAP(4),
    CAPACITY(5),
    ADAPTER_SAFETY(6),
    TARGET_POLICY_DRIFT(7);

    private final int wireValue;

    LaneRuntimeBlockReason(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static LaneRuntimeBlockReason fromWire(final long value) {
        for (LaneRuntimeBlockReason reason : values()) {
            if (reason.wireValue == value) {
                return reason;
            }
        }
        throw new IllegalArgumentException("unknown LaneRuntimeBlockReason: " + value);
    }
}

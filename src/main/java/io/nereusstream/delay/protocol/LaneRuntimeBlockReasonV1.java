package io.nereusstream.delay.protocol;

/** Closed V1 reason for a Lane whose runtime evidence is blocked. */
public enum LaneRuntimeBlockReasonV1 {
    CAPABILITY(1),
    CREDENTIAL_BINDING_DRIFT(2),
    DESTINATION_INCARNATION_MISMATCH(3),
    EVIDENCE_GAP(4),
    CAPACITY(5),
    ADAPTER_SAFETY(6),
    TARGET_POLICY_DRIFT(7);

    private final int wireValue;

    LaneRuntimeBlockReasonV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static LaneRuntimeBlockReasonV1 fromWire(final long value) {
        for (LaneRuntimeBlockReasonV1 reason : values()) {
            if (reason.wireValue == value) {
                return reason;
            }
        }
        throw new IllegalArgumentException("unknown LaneRuntimeBlockReasonV1: " + value);
    }
}

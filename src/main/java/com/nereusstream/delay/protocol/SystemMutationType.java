package com.nereusstream.delay.protocol;

/** Closed System Mutation operation values from Protocol Registry §2.1. */
public enum SystemMutationType {
    APPLY_SHARD_CONTROL(1),
    REPLAY_DEAD_LETTER(2),
    RESOLVE_UNCERTAIN(3),
    TIME_FENCE(4),
    PUBLISH_ADMISSION(5),
    PUBLISH_OUTCOME(6),
    EXPIRE_GENERATION(7),
    EVIDENCE_RESOLUTION(8),
    RESOURCE_RETIRE_INTENT(9),
    RESOURCE_DELETE_CONFIRMED(10),
    CLAIM_RESULT(11),
    DLQ_EXPORT_RESULT(12);

    private final int wireValue;

    SystemMutationType(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SystemMutationType fromWire(final long value) {
        for (SystemMutationType type : values()) {
            if (type.wireValue == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown SystemMutationType: " + value);
    }
}

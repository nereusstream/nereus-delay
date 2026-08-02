package io.nereusstream.delay.protocol;

/** Closed reason categories for source-ordered control markers. */
public enum ControlReasonKindV1 {
    OPERATOR_REQUEST(1),
    POLICY_CHANGE(2),
    CAPABILITY_REPLACEMENT(3),
    INCIDENT(4),
    TENANT_OFFBOARD(5),
    QUOTA_REALLOCATION(6),
    MAINTENANCE(7);

    private final int wireValue;

    ControlReasonKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlReasonKindV1 fromWire(final long value) {
        for (ControlReasonKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlReasonKindV1: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Closed reason categories for source-ordered control markers. */
public enum ControlReasonKind {
    OPERATOR_REQUEST(1),
    POLICY_CHANGE(2),
    CAPABILITY_REPLACEMENT(3),
    INCIDENT(4),
    TENANT_OFFBOARD(5),
    QUOTA_REALLOCATION(6),
    MAINTENANCE(7);

    private final int wireValue;

    ControlReasonKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlReasonKind fromWire(final long value) {
        for (ControlReasonKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown ControlReasonKind: " + value);
    }
}

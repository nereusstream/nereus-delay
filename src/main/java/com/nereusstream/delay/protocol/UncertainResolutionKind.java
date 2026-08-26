package com.nereusstream.delay.protocol;

/** Closed resolution choices for one uncertain publish attempt. */
public enum UncertainResolutionKind {
    ATTACH_PUBLISHED_EVIDENCE(1),
    ATTACH_NOT_PUBLISHED_EVIDENCE(2),
    RETRY_ALLOW_POSSIBLE_DUPLICATE(3),
    TERMINALIZE_POSSIBLE_DELIVERY(4);

    private final int wireValue;

    UncertainResolutionKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UncertainResolutionKind fromWire(final long value) {
        for (UncertainResolutionKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown UncertainResolutionKind: " + value);
    }
}

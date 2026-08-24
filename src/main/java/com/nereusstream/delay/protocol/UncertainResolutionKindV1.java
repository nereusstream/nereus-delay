package com.nereusstream.delay.protocol;

/** Closed resolution choices for one uncertain publish attempt. */
public enum UncertainResolutionKindV1 {
    ATTACH_PUBLISHED_EVIDENCE(1),
    ATTACH_NOT_PUBLISHED_EVIDENCE(2),
    RETRY_ALLOW_POSSIBLE_DUPLICATE(3),
    TERMINALIZE_POSSIBLE_DELIVERY(4);

    private final int wireValue;

    UncertainResolutionKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UncertainResolutionKindV1 fromWire(final long value) {
        for (UncertainResolutionKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown UncertainResolutionKindV1: " + value);
    }
}

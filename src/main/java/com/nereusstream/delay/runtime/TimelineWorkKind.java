package com.nereusstream.delay.runtime;

/** Registry {@code TimelineWorkKindV1} values. */
public enum TimelineWorkKind {
    INITIAL_SCHEDULE(1),
    DEFINITIVE_RETRY(2),
    UNCERTAIN_RETRY(3);

    private final int wireValue;

    TimelineWorkKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static TimelineWorkKind fromWire(final long value) {
        for (TimelineWorkKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown timeline work kind: " + value);
    }
}

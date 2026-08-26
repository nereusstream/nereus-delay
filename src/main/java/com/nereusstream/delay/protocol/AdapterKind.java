package com.nereusstream.delay.protocol;

/** Closed destination adapter kinds exposed by public query projections. */
public enum AdapterKind {
    KAFKA(1),
    PULSAR(2);

    private final int wireValue;

    AdapterKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static AdapterKind fromWire(final long value) {
        for (AdapterKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown AdapterKind: " + value);
    }
}

package io.nereusstream.delay.protocol;

/** Closed destination adapter kinds exposed by public query projections. */
public enum AdapterKindV1 {
    KAFKA(1),
    PULSAR(2);

    private final int wireValue;

    AdapterKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static AdapterKindV1 fromWire(final long value) {
        for (AdapterKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown AdapterKindV1: " + value);
    }
}

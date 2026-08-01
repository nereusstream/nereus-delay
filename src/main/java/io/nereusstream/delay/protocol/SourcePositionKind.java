package io.nereusstream.delay.protocol;

public enum SourcePositionKind {
    KAFKA(1),
    PULSAR(2);

    private final int wireValue;

    SourcePositionKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}


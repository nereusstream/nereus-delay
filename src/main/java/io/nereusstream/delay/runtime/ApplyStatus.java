package io.nereusstream.delay.runtime;

public enum ApplyStatus {
    APPLIED(1),
    REJECTED(2);

    private final int wireValue;

    ApplyStatus(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}


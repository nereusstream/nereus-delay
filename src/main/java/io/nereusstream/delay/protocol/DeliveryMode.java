package io.nereusstream.delay.protocol;

public enum DeliveryMode {
    MANAGED(1);

    private final int wireValue;

    DeliveryMode(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}


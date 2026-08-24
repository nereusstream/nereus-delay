package com.nereusstream.delay.protocol;

public enum DeliveryMode {
    MANAGED(1);

    private final int wireValue;

    DeliveryMode(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static DeliveryMode fromWire(final long value) {
        for (DeliveryMode mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown DeliveryMode: " + value);
    }
}

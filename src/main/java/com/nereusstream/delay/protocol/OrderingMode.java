package com.nereusstream.delay.protocol;

public enum OrderingMode {
    BEST_EFFORT(1),
    DELIVERY_TIME_FIFO(2);

    private final int wireValue;

    OrderingMode(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static OrderingMode fromWire(final long value) {
        for (OrderingMode mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown OrderingMode: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Physical timing contract selected by an admitted publish. */
public enum DeliveryContract {
    NEREUS_MANAGED_NOT_BEFORE(1),
    PULSAR_NATIVE_DELIVERY(2);

    private final int wireValue;

    DeliveryContract(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public boolean isNative() {
        return this == PULSAR_NATIVE_DELIVERY;
    }

    public static DeliveryContract fromWire(final long value) {
        for (DeliveryContract contract : values()) {
            if (contract.wireValue == value) {
                return contract;
            }
        }
        throw new IllegalArgumentException("unknown DeliveryContract: " + value);
    }
}

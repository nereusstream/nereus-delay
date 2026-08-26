package com.nereusstream.delay.protocol;

/** Closed destination outcome-capability registry from Protocol Registry §5.1.1. */
public enum OutcomeCapability {
    AT_LEAST_ONCE(1),
    KAFKA_TRANSACTIONAL_RECEIPT(2),
    PULSAR_BROKER_DEDUP(3);

    private final int wireValue;

    OutcomeCapability(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static OutcomeCapability fromWire(final long value) {
        for (OutcomeCapability capability : values()) {
            if (capability.wireValue == value) {
                return capability;
            }
        }
        throw new IllegalArgumentException("unknown OutcomeCapability: " + value);
    }
}

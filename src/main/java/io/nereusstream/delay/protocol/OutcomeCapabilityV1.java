package io.nereusstream.delay.protocol;

/** Closed destination outcome-capability registry from Protocol Registry §5.1.1. */
public enum OutcomeCapabilityV1 {
    AT_LEAST_ONCE(1),
    KAFKA_TRANSACTIONAL_RECEIPT(2),
    PULSAR_BROKER_DEDUP(3);

    private final int wireValue;

    OutcomeCapabilityV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static OutcomeCapabilityV1 fromWire(final long value) {
        for (OutcomeCapabilityV1 capability : values()) {
            if (capability.wireValue == value) {
                return capability;
            }
        }
        throw new IllegalArgumentException("unknown OutcomeCapabilityV1: " + value);
    }
}

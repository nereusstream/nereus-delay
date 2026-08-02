package io.nereusstream.delay.protocol;

/** Closed Object Store provider registry for immutable semantic Profiles. */
public enum ObjectStoreProviderKindV1 {
    S3(1),
    GCS(2),
    AZURE_BLOB(3),
    S3_COMPATIBLE(4);

    private final int wireValue;

    ObjectStoreProviderKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ObjectStoreProviderKindV1 fromWire(final long value) {
        for (ObjectStoreProviderKindV1 provider : values()) {
            if (provider.wireValue == value) {
                return provider;
            }
        }
        throw new IllegalArgumentException("unknown ObjectStoreProviderKindV1: " + value);
    }
}

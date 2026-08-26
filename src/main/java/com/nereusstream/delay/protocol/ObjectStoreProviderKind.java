package com.nereusstream.delay.protocol;

/** Closed Object Store provider registry for immutable semantic Profiles. */
public enum ObjectStoreProviderKind {
    S3(1),
    GCS(2),
    AZURE_BLOB(3),
    S3_COMPATIBLE(4);

    private final int wireValue;

    ObjectStoreProviderKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ObjectStoreProviderKind fromWire(final long value) {
        for (ObjectStoreProviderKind provider : values()) {
            if (provider.wireValue == value) {
                return provider;
            }
        }
        throw new IllegalArgumentException("unknown ObjectStoreProviderKind: " + value);
    }
}

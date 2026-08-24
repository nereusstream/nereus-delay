package com.nereusstream.delay.protocol;

/** Closed credential-use lease scopes registered by V1. */
public enum CredentialUseKindV1 {
    DESTINATION_CHANNEL(1),
    OBJECT_STORE_ADAPTER(2);

    private final int wireValue;

    CredentialUseKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CredentialUseKindV1 fromWire(final long value) {
        for (CredentialUseKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown CredentialUseKindV1: " + value);
    }
}

package com.nereusstream.delay.protocol;

/** Closed credential-use lease scopes registered by the current design. */
public enum CredentialUseKind {
    DESTINATION_CHANNEL(1),
    OBJECT_STORE_ADAPTER(2);

    private final int wireValue;

    CredentialUseKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CredentialUseKind fromWire(final long value) {
        for (CredentialUseKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown CredentialUseKind: " + value);
    }
}

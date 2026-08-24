package com.nereusstream.delay.protocol;

/** Closed opaque Object Store upload handle kinds. */
public enum UploadHandleKindV1 {
    OPAQUE_SINGLE_PUT(1),
    OPAQUE_MULTIPART(2);

    private final int wireValue;

    UploadHandleKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UploadHandleKindV1 fromWire(final long value) {
        for (UploadHandleKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown UploadHandleKindV1: " + value);
    }
}

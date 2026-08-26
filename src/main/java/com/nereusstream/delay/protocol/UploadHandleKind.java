package com.nereusstream.delay.protocol;

/** Closed opaque Object Store upload handle kinds. */
public enum UploadHandleKind {
    OPAQUE_SINGLE_PUT(1),
    OPAQUE_MULTIPART(2);

    private final int wireValue;

    UploadHandleKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static UploadHandleKind fromWire(final long value) {
        for (UploadHandleKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown UploadHandleKind: " + value);
    }
}

package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable value wrapper for a fixed-width binary identity. */
public abstract class FixedBytes {
    private final byte[] value;

    protected FixedBytes(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        this.value = Bytes.copy(value);
    }

    public final byte[] bytes() {
        return Bytes.copy(value);
    }

    protected final byte[] unsafeBytes() {
        return value;
    }

    @Override
    public final boolean equals(final Object other) {
        return this == other || other != null && getClass() == other.getClass()
                && Arrays.equals(value, ((FixedBytes) other).value);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return Bytes.hex(value);
    }

    protected static byte[] requireNonNull(final byte[] value) {
        return Objects.requireNonNull(value, "value");
    }
}


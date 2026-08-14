package io.nereusstream.delay.transport;

import io.nereusstream.delay.protocol.FixedBytes;

/** Immutable 16-byte value used for a physical transport attempt. */
public class Bytes16 extends FixedBytes {
    public static final int LENGTH = 16;

    public Bytes16(final byte[] value) {
        super(value, LENGTH, "Bytes16");
    }
}

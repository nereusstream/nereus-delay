package io.nereusstream.delay.transport;

import io.nereusstream.delay.protocol.FixedBytes;

/** Immutable 32-byte value used for physical resource identities. */
public final class Bytes32 extends FixedBytes {
    public static final int LENGTH = 32;

    public Bytes32(final byte[] value) {
        super(value, LENGTH, "Bytes32");
    }
}

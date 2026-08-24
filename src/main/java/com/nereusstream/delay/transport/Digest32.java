package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.FixedBytes;

/** Immutable SHA-256-sized digest value used as a map/record key. */
public final class Digest32 extends FixedBytes {
    public static final int LENGTH = 32;

    public Digest32(final byte[] value) {
        super(value, LENGTH, "Digest32");
    }
}

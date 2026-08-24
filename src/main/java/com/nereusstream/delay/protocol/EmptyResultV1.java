package com.nereusstream.delay.protocol;

/** Empty branch used where the public result has no retained details. */
public final class EmptyResultV1 implements QueryResponseBranchV1 {
    public static final EmptyResultV1 INSTANCE = new EmptyResultV1();

    private EmptyResultV1() {}

    public byte[] canonicalBytes() {
        return new byte[0];
    }

    public static EmptyResultV1 decode(final byte[] encoded) {
        if (encoded.length != 0) {
            throw new IllegalArgumentException("EmptyResultV1 must have no fields");
        }
        return INSTANCE;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EmptyResultV1;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}

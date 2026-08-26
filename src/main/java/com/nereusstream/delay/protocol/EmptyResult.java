package com.nereusstream.delay.protocol;

/** Empty branch used where the public result has no retained details. */
public final class EmptyResult implements QueryResponseBranch {
    public static final EmptyResult INSTANCE = new EmptyResult();

    private EmptyResult() {}

    public byte[] canonicalBytes() {
        return new byte[0];
    }

    public static EmptyResult decode(final byte[] encoded) {
        if (encoded.length != 0) {
            throw new IllegalArgumentException("EmptyResult must have no fields");
        }
        return INSTANCE;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EmptyResult;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}

package io.nereusstream.delay.protocol;

/** Empty projection proving that a Message identity was retired. */
public final class IdentityRetiredMessageViewV1 implements QueryResponseBranchV1 {
    public static final IdentityRetiredMessageViewV1 INSTANCE = new IdentityRetiredMessageViewV1();

    private IdentityRetiredMessageViewV1() {
    }

    public byte[] canonicalBytes() {
        return new byte[0];
    }

    public static IdentityRetiredMessageViewV1 decode(final byte[] encoded) {
        if (encoded.length != 0) {
            throw new IllegalArgumentException("IdentityRetiredMessageViewV1 must have no fields");
        }
        return INSTANCE;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof IdentityRetiredMessageViewV1;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}

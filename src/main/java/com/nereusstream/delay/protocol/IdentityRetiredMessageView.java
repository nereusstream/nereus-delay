package com.nereusstream.delay.protocol;

/** Empty projection proving that a Message identity was retired. */
public final class IdentityRetiredMessageView implements QueryResponseBranch {
    public static final IdentityRetiredMessageView INSTANCE = new IdentityRetiredMessageView();

    private IdentityRetiredMessageView() {}

    public byte[] canonicalBytes() {
        return new byte[0];
    }

    public static IdentityRetiredMessageView decode(final byte[] encoded) {
        if (encoded.length != 0) {
            throw new IllegalArgumentException("IdentityRetiredMessageView must have no fields");
        }
        return INSTANCE;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof IdentityRetiredMessageView;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}

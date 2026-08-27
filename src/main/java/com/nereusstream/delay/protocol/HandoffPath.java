package com.nereusstream.delay.protocol;

/** Bit registry for the two independently gated native handoff paths. */
public final class HandoffPath {
    public static final int MANAGED_HANDOFF = 0x01;
    public static final int AUTO_FAST = 0x02;
    public static final int VALID_MASK = MANAGED_HANDOFF | AUTO_FAST;

    private HandoffPath() {}

    public static void requireValid(final int bits) {
        if ((bits & ~VALID_MASK) != 0) {
            throw new IllegalArgumentException("unknown HandoffPath bits");
        }
    }

    public static boolean includes(final int bits, final int path) {
        requireValid(bits);
        if (path <= 0 || (path & ~VALID_MASK) != 0) {
            throw new IllegalArgumentException("unknown HandoffPath bit");
        }
        return (bits & path) == path;
    }
}
